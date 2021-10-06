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
from exceptions import FailedInitialization, WatchdogTimer, InfluxDBWriteError

_LOGGER = logging.getLogger('esphome')


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

    def __init__(self, config):
        """Create a new CS object."""
        self._config = config
        self._task_gather = None
        self._esphome = None
        self._name = None
        self._sampler_queue = None
        self._sensors_by_name = None
        self._sensors_by_key = None
        CircuitSetup._INFLUX = InfluxDB()

    async def start(self):
        """Initialize the CS ESPHome API."""
        config = self._config

        if 'influxdb2' in config.keys():
            if not CircuitSetup._INFLUX.start(config=config.influxdb2):
                return False

        try:
            url = config.circuitsetup.url
            port = config.circuitsetup.get('port', CircuitSetup._DEFAULT_ESPHOME_API_PORT)
            password = config.circuitsetup.get('password', CircuitSetup._DEFAULT_ESPHOME_API_PASSWORD)
            self._esphome = aioesphomeapi.APIClient(eventloop=asyncio.get_running_loop(), address=url, port=port, password=password)
            await self._esphome.connect(login=True)
        except SocketAPIError as e:
            _LOGGER.error(f"{e}")
            return False
        except InvalidAuthAPIError as e:
            _LOGGER.error(f"ESPHome login failed: {e}")
            return False
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
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
            self._sampler_queue = asyncio.Queue()
            self._task_gather = asyncio.gather(
                self.midnight(),
                self.sampler_task(),
                self.posting_task(),
                self.watchdog(),
            )
            await self._task_gather
        except InfluxDBWriteError as e:
            _LOGGER.error(f"{e}")
        except Exception as e:
            _LOGGER.error(f"something else: {e}")

    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            now = datetime.datetime.now()
            tomorrow = now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 5))
            await asyncio.sleep((midnight - now).total_seconds())

            # Update internal sun info and the daily production
            _LOGGER.info(f"esphome energy collection utility {version.get_version()}, PID is {os.getpid()}")

    async def posting_task(self):
        """Process the subscribed data."""
        while True:
            data = await self._sampler_queue.get()
            sensor = data.get('sensor', None)
            state = data.get('state', None)
            self._sampler_queue.task_done()
            if sensor and state and CircuitSetup._INFLUX:
                ts = data.get('ts', None)
                CircuitSetup._INFLUX.write_sensor(sensor=sensor, state=state, timestamp=ts)

    async def sampler_task(self):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == aioesphomeapi.SensorState:
                sensor = self._sensors_by_key.get(state.key, None)
                self._sampler_queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': int(time.time())})

        await self._esphome.subscribe_states(sensor_callback)

    _WATCHDOG = 0
    async def watchdog(self):
        """Check that we are connected to the CircuitSetup hardware."""
        saved_watchdog = CircuitSetup._WATCHDOG
        while True:
            await asyncio.sleep(60)
            current_watchdog = CircuitSetup._WATCHDOG
            if saved_watchdog == current_watchdog:
                raise WatchdogTimer(f"Lost connection to {self._name}")
            saved_watchdog = current_watchdog

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
