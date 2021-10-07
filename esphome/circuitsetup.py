"""Code to interface with the CircuitSetup 6-channel energy monitor."""

import os
import time
import asyncio
import logging
import datetime

import aioesphomeapi
from aioesphomeapi.core import SocketAPIError, InvalidAuthAPIError

import version
from influx import InfluxDB
from exceptions import FailedInitialization, WatchdogTimer

_LOGGER = logging.getLogger('esphome')

# Default sampling values used when not defined in the configuration file
_DEFAULT_FAST = 30
_DEFAULT_MEDIUM = 60
_DEFAULT_SLOW = 120


def parse_sensors(yaml, entities):
    sensors_by_name = {}
    sensors_by_key = {}
    keys_by_name = dict((sensor.name, sensor.key) for sensor in entities)
    units_by_name = dict((sensor.name, sensor.unit_of_measurement) for sensor in entities)
    decimals_by_name = dict((sensor.name, sensor.accuracy_decimals) for sensor in entities)
    for entry in yaml:
        for details in entry.values():
            enable = details.get('enable', True)
            sensor_name = details.get('sensor_name', None)
            key = keys_by_name.get(sensor_name, None)
            if key and enable:
                now = time.time()
                data = {
                    'sensor_name': details.get('sensor_name', None),
                    'display_name': details.get('display_name', None),
                    'unit': units_by_name.get(sensor_name, None),
                    'key': keys_by_name.get(sensor_name, None),
                    'precision': decimals_by_name.get(sensor_name, None),
                    'measurement': details.get('measurement', None),
                    'device': details.get('device', None),
                    'location': details.get('location', None),
                }
                sensors_by_name[sensor_name] = data
                sensors_by_key[key] = data

    return sensors_by_name, sensors_by_key


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _INFLUX = None
    _DEFAULT_ESPHOME_API_PORT = 6053
    _DEFAULT_ESPHOME_API_PASSWORD = ''
    _WATCHDOG = 0

    def __init__(self, config):
        """Create a new CircuitSetup object."""
        self._config = config
        self._task_gather = None
        self._esphome = None
        self._name = None
        self._sensors_by_name = None
        self._sensors_by_key = None
        self._sampling_fast = _DEFAULT_FAST
        self._sampling_medium = _DEFAULT_MEDIUM
        self._sampling_slow = _DEFAULT_SLOW

    async def start(self):
        """Initialize the CS ESPHome API."""
        config = self._config

        if 'settings' in config.keys() and 'sampling' in config.settings.keys():
            self._sampling_fast = config.settings.sampling.get('fast', _DEFAULT_FAST)
            self._sampling_medium = config.settings.sampling.get('medium', _DEFAULT_MEDIUM)
            self._sampling_slow = config.settings.sampling.get('slow', _DEFAULT_SLOW)

        if 'influxdb2' in config.keys():
            CircuitSetup._INFLUX = InfluxDB(config)
            if not CircuitSetup._INFLUX.start():
                CircuitSetup._INFLUX = None
                return False

        try:
            success = False
            url = config.circuitsetup.url
            port = config.circuitsetup.get('port', CircuitSetup._DEFAULT_ESPHOME_API_PORT)
            password = config.circuitsetup.get('password', CircuitSetup._DEFAULT_ESPHOME_API_PASSWORD)
            self._esphome = aioesphomeapi.APIClient(eventloop=asyncio.get_running_loop(), address=url, port=port, password=password)
            await self._esphome.connect(login=True)
            success = True
        except SocketAPIError as e:
            _LOGGER.error(f"{e}")
        except InvalidAuthAPIError as e:
            _LOGGER.error(f"ESPHome login failed: {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
        finally:
            if not success:
                self._esphome = None
                return False

        try:
            api_version = self._esphome.api_version
            _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")

            device_info = await self._esphome.device_info()
            self._name = device_info.name
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}")
            _LOGGER.info(f"ESPHome version: {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing version and/or device_info: {e}")
            return False

        try:
            entities, services = await self._esphome.list_entities_services()
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing '{self._name}' list_entities_services(): {e}")
            return False

        self._sensors_by_name, self._sensors_by_key = parse_sensors(yaml=config.sensors, entities=entities)
        return True

    async def run(self):
        try:
            queues = {
                'sampler': asyncio.Queue(),
                'fast': asyncio.Queue(),
                'medium': asyncio.Queue(),
                'slow': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self.midnight(),
                self.watchdog(),
                self.scheduler(queues),
                self.task_fast(queues.get('fast')),
                self.task_medium(queues.get('medium')),
                self.task_slow(queues.get('slow')),
                self.task_sampler(queues.get('sampler')),
                self.posting_task(queues.get('sampler')),
            )
            await self._task_gather
        except Exception as e:
            _LOGGER.error(f"something else: {e}")

    async def stop(self):
        """Shutdown."""
        if self._task_gather:
            self._task_gather.cancel()

        if self._esphome:
            await self._esphome.disconnect()
            self._esphome = None
            await asyncio.sleep(0.25)
        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None

    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            now = datetime.datetime.now()
            tomorrow = now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 1))
            await asyncio.sleep((midnight - now).total_seconds())

            # Update internal sun info and the daily production
            _LOGGER.info(f"esphome energy collection utility {version.get_version()}, PID is {os.getpid()}")

    async def watchdog(self):
        """Check that we are connected to the CircuitSetup hardware."""
        saved_watchdog = CircuitSetup._WATCHDOG
        while True:
            await asyncio.sleep(60)
            current_watchdog = CircuitSetup._WATCHDOG
            if saved_watchdog == current_watchdog:
                raise WatchdogTimer(f"Lost connection to {self._name}")
            saved_watchdog = current_watchdog

    async def scheduler(self, queues):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = time.time_ns() // 1000000000
        while True:
            tick = time.time_ns() // 1000000000
            if tick != last_tick:
                last_tick = tick
                if tick % self._sampling_fast == 0:
                    queues.get('fast').put_nowait(tick)
                if tick % self._sampling_medium == 0:
                    queues.get('medium').put_nowait(tick)
                if tick % self._sampling_slow == 0:
                    queues.get('slow').put_nowait(tick)

            await asyncio.sleep(SLEEP)

    async def task_fast(self, queue):
        """Work done at a fast sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_fast(queue)")

    async def task_medium(self, queue):
        """Work done at a medium sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_medium(queue)")

            query_api = CircuitSetup._INFLUX.query_api()
            bucket = CircuitSetup._INFLUX.bucket()
            try:
                midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                for name in ['cs24_w', 'cs24_ct14_w', 'cs24_ct33_w']:
                    sensor = self._sensors_by_name.get(name)
                    location = sensor.get('location')
                    device = sensor.get('device')
                    measurement = sensor.get('measurement')
                    query = f'from(bucket: "{bucket}")' \
                    f' |> range(start: {midnight})' \
                    f' |> filter(fn: (r) => r["_measurement"] == "{measurement}")' \
                    f' |> filter(fn: (r) => r["_field"] == "{device}")' \
                    f' |> filter(fn: (r) => r["_location"] == "{location}")' \
                    f' |> integral(unit: 1h, column: "_value")'
                    tables = query_api.query(query)
                    for table in tables:
                        for row in table.records:
                            #_LOGGER.info(f"{device}: {row.values.get('_value'):.3f} Wh")
                            value = row.values.get('_value')
                            CircuitSetup._INFLUX.write_point(measurement, {'t': '_integral', 'v': 'today'}, device, value, timestamp=midnight)
            except Exception as e:
                _LOGGER.info(f"{e}")

    async def task_slow(self, queue):
        """Work done at a slow sample rate."""
        while True:
            timestamp = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_slow(queue)")

    async def posting_task(self, queue):
        """Process the subscribed data."""
        while True:
            packet = await queue.get()
            sensor = packet.get('sensor', None)
            state = packet.get('state', None)
            queue.task_done()
            if sensor and state and CircuitSetup._INFLUX:
                ts = packet.get('ts', None)
                try:
                    CircuitSetup._INFLUX.write_sensor(sensor=sensor, state=state, timestamp=ts)
                except Exception as e:
                    _LOGGER.warning(f"{e}")

    async def task_sampler(self, queue):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == aioesphomeapi.SensorState:
                sensor = self._sensors_by_key.get(state.key, None)
                queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': int(time.time())})

        await self._esphome.subscribe_states(sensor_callback)
