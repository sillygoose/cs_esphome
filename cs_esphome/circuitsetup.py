"""Code to interface with the CircuitSetup 6-channel energy monitor using ESPHome."""

import os
import time
import asyncio
import logging
import datetime

from dateutil.relativedelta import relativedelta
from dateutil import tz
import pytz

import aioesphomeapi
from aioesphomeapi.core import SocketAPIError, InvalidAuthAPIError

import version
import sensors
import query
from readconfig import retrieve_options

from influx import InfluxDB
from exceptions import WatchdogTimer, InfluxDBFormatError, InfluxDBWriteError, InternalError

_LOGGER = logging.getLogger('cs_esphome')


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _INFLUX = None
    _DEFAULT_ESPHOME_API_PORT = 6053
    _DEFAULT_ESPHOME_API_PASSWORD = ''
    _WATCHDOG = 0

    _DEFAULT_INTEGRATIONS = 30
    _DEFAULT_DELETIONS = 60 * 60 * 24

    def __init__(self, config):
        """Create a new CircuitSetup object."""
        self._config = config
        self._task_gather = None
        self._esphome = None
        self._name = None
        self._sensor_by_name = None
        self._sensor_by_key = None
        self._sensor_integrate = None
        self._sensor_locations = None
        self._sampling_integrations = CircuitSetup._DEFAULT_INTEGRATIONS
        self._sampling_deletions = CircuitSetup._DEFAULT_DELETIONS

    async def start(self):
        """Initialize the CS ESPHome API."""
        config = self._config

        if 'settings' in config.keys() and 'sampling' in config.settings.keys():
            self._sampling_integrations = config.settings.sampling.get('integrations', CircuitSetup._DEFAULT_INTEGRATIONS)
            self._sampling_deletions = config.settings.sampling.get('deletions', CircuitSetup._DEFAULT_DELETIONS)

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
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}, version {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing version and/or device_info: {e}")
            return False

        try:
            entities, services = await self._esphome.list_entities_services()
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing '{self._name}' list_entities_services(): {e}")
            return False

        self._sensor_by_name, self._sensor_by_key = sensors.parse_sensors(yaml=config.sensors, entities=entities)
        self._sensor_locations = sensors.parse_by_location(self._sensor_by_name)
        self._sensor_integrate = sensors.parse_by_integration(self._sensor_by_name)
        return True

    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome starting up, integrations running every {self._sampling_integrations} seconds, deletions running every {self._sampling_deletions} seconds")
            queues = {
                'sampler': asyncio.Queue(),
                'integrations': asyncio.Queue(),
                'deletions': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self.midnight(),
                self.watchdog(),
                self.filldata(),
                self.scheduler(queues),
                self.task_integrations(queues.get('integrations')),
                self.task_deletions(queues.get('deletions')),
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
            await asyncio.sleep(0.5)
        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None


    async def filldata(self) -> None:
        """Task to fill in missing data for Grafana."""
        _DEBUG_ENV_VAR = 'CS_ESPHOME_DEBUG'
        _DEBUG_OPTIONS = {
            'fill_data': {'type': bool, 'required': False},
        }
        debug_options = retrieve_options(self._config, 'debug', _DEBUG_OPTIONS)
        cs_esphome_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
        if cs_esphome_debug == False or debug_options.get('fill_data', False) == False:
            return

        local_start = datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)) - relativedelta(months=13)
        local_stop = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0))
        utc_start = pytz.utc.localize(local_start)
        utc_stop = pytz.utc.localize(local_stop)

        query_api = CircuitSetup._INFLUX.query_api()
        bucket = CircuitSetup._INFLUX.bucket()
        check_query = f'from(bucket: "{bucket}")' \
            f' |> range(start: 0)' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f' |> filter(fn: (r) => r._device == "line")' \
            f' |> filter(fn: (r) => r._field == "today")' \
            f' |> first()'
        tables = query.execute_query(query_api, check_query)
        for table in tables:
            for row in table.records:
                stop = row.values.get('_time')

        utc_current = utc_start
        local_current = local_start
        while utc_current < utc_stop:
            CircuitSetup._INFLUX.write_point('energy', [{'t': '_device', 'v': 'line'}], 'today', 0.0, int(local_current.timestamp()))
            utc_current += relativedelta(days=1)
            local_current += relativedelta(days=1)

        utc_current = utc_start
        local_current = local_start
        while utc_current < utc_stop:
            CircuitSetup._INFLUX.write_point('energy', [{'t': '_device', 'v': 'line'}], 'month', 0.0, int(local_current.timestamp()))
            utc_current += relativedelta(months=1)
            local_current += relativedelta(months=1)

        utc_current = utc_start
        local_current = local_start
        while utc_current < utc_stop:
            CircuitSetup._INFLUX.write_point('energy', [{'t': '_device', 'v': 'line'}], 'year', 0.0, int(local_current.timestamp()))
            utc_current += relativedelta(years=1)
            local_current += relativedelta(years=1)

        _LOGGER.info(f"CS/ESPHome missing data fill: {local_start.date()} to {local_stop.date()}")


    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            now = datetime.datetime.now()
            tomorrow = now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 1))
            await asyncio.sleep((midnight - now).total_seconds())
            _LOGGER.info(f"CS/ESPHome energy collection utility {version.get_version()}, PID is {os.getpid()}")


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
                if tick % self._sampling_integrations == 0:
                    queues.get('integrations').put_nowait(tick)
                if tick % self._sampling_deletions == 0:
                    queues.get('deletions').put_nowait(tick)
            await asyncio.sleep(SLEEP)


    async def task_integrations(self, queue):
        """Task that processes the device and location integrations."""
        query_api = CircuitSetup._INFLUX.query_api()
        bucket = CircuitSetup._INFLUX.bucket()
        while True:
            timestamp = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_integrations(queue)")
            try:
                today = query.integrate_locations(query_api, bucket, self._sensor_locations, 'today')
                today += query.integrate_devices(query_api, bucket, self._sensor_integrate, 'today')
                CircuitSetup._INFLUX.write_points(today)

                month = query.integrate_locations(query_api, bucket, self._sensor_locations, 'month')
                month += query.integrate_devices(query_api, bucket, self._sensor_integrate, 'month')
                CircuitSetup._INFLUX.write_points(month)

                year = query.integrate_locations(query_api, bucket, self._sensor_locations, 'year')
                year += query.integrate_devices(query_api, bucket, self._sensor_integrate, 'year')
                CircuitSetup._INFLUX.write_points(year)
            except InfluxDBWriteError as e:
                _LOGGER.warning(f"{e}")
            except InternalError as e:
                _LOGGER.error(f"Internal error detected, {e}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception: {e}")


    async def task_deletions(self, queue):
        """Work done at a slow sample rate."""
        delete_api = CircuitSetup._INFLUX.delete_api()
        bucket = CircuitSetup._INFLUX.bucket()
        org = CircuitSetup._INFLUX.org()
        while True:
            timestamp = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_deletions(queue)")



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
                except InfluxDBFormatError as e:
                    _LOGGER.warning(f"{e}")


    async def task_sampler(self, queue):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == aioesphomeapi.SensorState:
                sensor = self._sensor_by_key.get(state.key, None)
                queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': int(time.time())})

        await self._esphome.subscribe_states(sensor_callback)
