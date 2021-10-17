"""Code to interface with the CircuitSetup 6-channel energy monitor using ESPHome."""

import os
import time
import asyncio
import logging
import datetime
import json

from dateutil.relativedelta import relativedelta

import aioesphomeapi
from aioesphomeapi.core import SocketAPIError, InvalidAuthAPIError

import version
import sensors
import query
from readconfig import retrieve_options

from influx import InfluxDB
from influxdb_client.rest import ApiException
from exceptions import WatchdogTimer, InfluxDBFormatError, InfluxDBWriteError, InternalError, FailedInitialization

_LOGGER = logging.getLogger('cs_esphome')


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _INFLUX = None
    _DEFAULT_ESPHOME_API_PORT = 6053
    _DEFAULT_ESPHOME_API_PASSWORD = ''

    _DEFAULT_WATCHDOG = 60
    _WATCHDOG = 0

    _DEFAULT_INTEGRATIONS = 30

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
        self._watchdog = CircuitSetup._DEFAULT_WATCHDOG

    async def start(self):
        """Initialize the CS/ESPHome API."""
        config = self._config

        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_integrations = config.settings.sampling.get('integrations', CircuitSetup._DEFAULT_INTEGRATIONS)
            if 'watchdog' in config.settings.keys():
                self._watchdog = config.settings.get('watchdog', CircuitSetup._DEFAULT_WATCHDOG)

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
            self._esphome = aioesphomeapi.APIClient(address=url, port=port, password=password)
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

        if 'meter' in config.keys():
            if 'enable_setting' in config.meter.keys():
                enable_setting = config.meter.get('enable_setting', None)
                if enable_setting:
                    if 'value' in config.meter.keys():
                        value = config.meter.get('value', None)
                        right_now = datetime.datetime.now()
                        midnight = datetime.datetime.combine(right_now, datetime.time(0, 0))
                        CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'meter_reading'}], field='today', value=value, timestamp=int(midnight.timestamp()))


        self._sensor_by_name, self._sensor_by_key = sensors.parse_sensors(yaml=config.sensors, entities=entities)
        self._sensor_locations = sensors.parse_by_location(self._sensor_by_name)
        self._sensor_integrate = sensors.parse_by_integration(self._sensor_by_name)
        return True

    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome starting up, ESPHome sensors processed every {self._sampling_integrations} seconds")
            queues = {
                'sampler': asyncio.Queue(),
                'integrations': asyncio.Queue(),
                'deletions': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self.midnight(),
                self.watchdog(),
                self.filldata(),
                self.influx_tasks(),
                self.scheduler(queues),
                self.task_integrations(queues.get('integrations')),
                self.task_deletions(queues.get('deletions')),
                self.task_sampler(queues.get('sampler')),
                self.posting_task(queues.get('sampler')),
            )
            await self._task_gather
        except FailedInitialization as e:
            _LOGGER.error(f"{e}")
        except WatchdogTimer as e:
            _LOGGER.error(f"{e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")


    async def stop(self):
        """Shutdown."""
        if self._task_gather:
            self._task_gather.cancel()
            await asyncio.sleep(0.5)

        if self._esphome:
            await self._esphome.disconnect()
            self._esphome = None
            await asyncio.sleep(0.5)
        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None


    def influx_delta_wh_tasks(self, tasks_api, organization) -> None:
        task_base_name = 'delta_kwh'
        task_measurement = 'energy'
        task_device = 'delta_wh'

        periods = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        for period, range in periods.items():
            task_name = task_base_name + '_' + period
            tasks = tasks_api.find_tasks(name=task_name)
            if tasks is None or len(tasks) == 0:
                _LOGGER.info(f"InfluxDB task '{task_name}' was not found, creating...")
                task_organization = organization.name
                flux =  '\n' \
                        f'production_{period} = from(bucket: "multisma2")\n' \
                        f'  |> range(start: {range})\n' \
                        f'  |> filter(fn: (r) => r._measurement == "production" and r._field == "{period}" and r._inverter == "site")\n' \
                        f'  |> map(fn: (r) => ({{ r with _value: r._value * 1000.0 }}))\n' \
                        f'  |> rename(columns: {{_inverter: "_device"}})\n' \
                        f'  |> drop(columns: ["_start", "_stop", "_field", "_measurement"])\n' \
                        f'  |> yield(name: "production_{period}")\n' \
                        f'\n' \
                        f'consumption_{period} = from(bucket: "cs24")\n' \
                        f'  |> range(start: {range})\n' \
                        f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "line" and r._field == "{period}")\n' \
                        f'  |> drop(columns: ["_start", "_stop", "_field", "_measurement"])\n' \
                        f'  |> yield(name: "consumption_{period}")\n' \
                        f'\n' \
                        f'union(tables: [production_{period}, consumption_{period}])\n' \
                        f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")\n' \
                        f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{task_measurement}", _device: "{task_device}", _field: "{period}", _value: r.line - r.site }}))\n' \
                        f'  |> to(bucket: "cs24", org: "{task_organization}")\n' \
                        f'  |> yield(name: "delta_wh_{period}")\n'
                try:
                    tasks_api.create_task_every(name=task_name, flux=flux, every='5m', organization=organization)
                    _LOGGER.info(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation: {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation: {e}")


    ###
    def influx_meter_tasks(self, tasks_api, organization) -> None:
        task_name = 'net_meter'
        task_measurement = 'energy'
        task_device = 'meter_reading'

        tasks = tasks_api.find_tasks(name=task_name)
        if tasks is None or len(tasks) == 0:
            _LOGGER.info(f"InfluxDB task '{task_name}' was not found, creating...")
            flux =  '\n' \
                    f'from(bucket: "cs24")\n' \
                    f'  |> range(start: -1d)\n' \
                    f'  |> filter(fn: (r) => r._measurement == "energy" and r._field == "today")\n' \
                    f'  |> filter(fn: (r) => r._device == "meter_reading" or r._device == "delta_wh")\n' \
                    f'  |> drop(columns: ["_start", "_stop"])\n' \
                    f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{task_measurement}", _device: "{task_device}", _field: "today", _value: (r.delta_wh * 0.001) + r.meter_reading }}))\n' \
                    f'  |> yield(name: "meter_reading")\n'

            try:
                tasks_api.create_task_every(name=task_name, flux=flux, every='5m', organization=organization)
                _LOGGER.info(f"InfluxDB task '{task_name}' was successfully created")
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"ApiException during task creation: {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation: {e}")
        return
    ###


    async def influx_tasks(self) -> None:
        tasks_api = CircuitSetup._INFLUX.tasks_api()
        organizations_api = CircuitSetup._INFLUX.organizations_api()

        task_organization = CircuitSetup._INFLUX.org()
        try:
            organizations = organizations_api.find_organizations(org=task_organization)
        except ApiException as e:
            body_dict = json.loads(e.body)
            _LOGGER.info(f"Can't access InfluxDB organization: {body_dict.get('message', '???')}")
            return
        except Exception as e:
            _LOGGER.info(f"Can't create InfluxDB task: unexpected exception: {e}")
            return

        self.influx_delta_wh_tasks(tasks_api=tasks_api, organization=organizations[0])
        # self.influx_meter_tasks(tasks_api=tasks_api, organization=organizations[0])


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

        """Task to fill in missing data for Grafana."""
        _DEBUG_ENV_VAR = 'CS_ESPHOME_DEBUG'
        _DEBUG_OPTIONS = {
            'fill_data': {'type': bool, 'required': False},
        }
        debug_options = retrieve_options(self._config, 'debug', _DEBUG_OPTIONS)
        cs_esphome_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
        if cs_esphome_debug == False or debug_options.get('fill_data', False) == False:
            return

        start = datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)) - relativedelta(months=13)
        stop = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0))

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
                utc = row.values.get('_time')
                stop = datetime.datetime(year=utc.year, month=utc.month, day=utc.day)

        current = start.replace(month=1, day=1)
        while current < stop:
            CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='year', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(years=1)

        current = start.replace(day=1)
        while current < stop:
            CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='month', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(months=1)

        current = start
        while current < stop:
            CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='today', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(days=1)

        _LOGGER.info(f"CS/ESPHome missing data fill: {start.date()} to {stop.date()}")


    async def midnight(self) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            right_now = datetime.datetime.now()
            tomorrow = right_now + datetime.timedelta(days=1)
            midnight = datetime.datetime.combine(tomorrow, datetime.time(0, 0))
            await asyncio.sleep((midnight - right_now).total_seconds())

            _LOGGER.info(f"CS/ESPHome energy collection utility {version.get_version()}, PID is {os.getpid()}")

            query_api = CircuitSetup._INFLUX.query_api()
            bucket = CircuitSetup._INFLUX.bucket()
            read_query = f'from(bucket: "{bucket}")' \
                f' |> range(start: -25h)' \
                f' |> filter(fn: (r) => r._measurement == "energy")' \
                f' |> filter(fn: (r) => r._device == "meter_reading" or r._device == "delta_wh")' \
                f' |> filter(fn: (r) => r._field == "today")' \
                f' |> first()'

            try:
                tables = query.execute_query(query_api, read_query)
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"Problem with query: {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation: {e}")

            delta_wh = None
            meter_reading = None
            for table in tables:
                for row in table.records:
                    device = row.values.get('_device', None)
                    if device == 'meter_reading':
                        meter_reading = row.values.get('_value', None)
                    elif device == 'delta_wh':
                        delta_wh = row.values.get('_value', None)
                    else:
                        _LOGGER.error(f"Unexpected device: {device}")

            if meter_reading is not None and delta_wh is not None:
                delta_kwh = delta_wh * 0.001
                final_meter_reading = meter_reading + delta_kwh

                midnight_today = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0))
                midnight_yesterday = midnight_today - datetime.timedelta(days=1)
                _LOGGER.info(f"Final meter reading for {midnight_yesterday}: {final_meter_reading}")

                try:
                    CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'meter_reading'}], field='today', value=final_meter_reading, timestamp=int(midnight_yesterday.timestamp()))
                    CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'meter_reading'}], field='today', value=final_meter_reading, timestamp=int(midnight_today.timestamp()))
                except InfluxDBWriteError as e:
                    _LOGGER.warning(f"{e}")


    async def watchdog(self):
        """Check that we are connected to the CircuitSetup hardware."""
        saved_watchdog = CircuitSetup._WATCHDOG
        while True:
            await asyncio.sleep(self._watchdog)
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
        #start = datetime.datetime.combine(datetime.datetime.now().replace(month=8, day=1), datetime.time(0, 0))
        #stop = datetime.datetime.combine(datetime.datetime.now().replace(month=9, day=3), datetime.time(0, 0))
        #start = "2020-01-01T00:00:00Z"
        #stop = "2021-10-03T00:00:00Z"
        #delete_api.delete(start, stop, '_measurement="energy"', bucket=bucket, org=org)
        while True:
            request = await queue.get()
            queue.task_done()
            _LOGGER.debug(f"task_deletions(queue): {request}")


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
                ts = (int(time.time()) // 10) * 10
                sensor = self._sensor_by_key.get(state.key, None)
                queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': ts})

        await self._esphome.subscribe_states(sensor_callback)
