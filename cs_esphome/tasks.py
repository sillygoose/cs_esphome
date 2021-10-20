"""InfluxDB task management for CS/ESPHome."""

import logging
import json
import datetime

from influxdb_client.rest import ApiException
from exceptions import FailedInitialization, InternalError, InfluxDBWriteError  # WatchdogTimer, InfluxDBFormatError, ,


_LOGGER = logging.getLogger('cs_esphome')


class TaskManager():
    """Class to create and manage InfluxDB tasks."""

    _DEFAULT_INTEGRATIONS = 30

    def __init__(self, config, influxdb_client):
        """Create a new TaskManager object."""
        self._config = config
        self._client = influxdb_client
        self._task_gather = None
        self._organizations_api = None
        self._tasks_api = None
        self._query_api = None
        self._bucket = None
        self._sampling_integrations = None
        self._sensors_by_integration = None
        self._sensors_by_location = None


    async def start(self, by_location, by_integration) -> bool:
        """Initialize the task manager"""

        self._sensors_by_integration = by_integration
        self._sensors_by_location = by_location
        client = self._client

        self._organizations_api = client.organizations_api()
        self._tasks_api = client.tasks_api()
        self._query_api = client.query_api()
        self._bucket = client.bucket()

        self._sampling_integrations = TaskManager._DEFAULT_INTEGRATIONS
        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_integrations = config.settings.sampling.get('integrations', TaskManager._DEFAULT_INTEGRATIONS)
        if 'debug' in config.keys():
            if 'recreate_tasks' in config.debug.keys():
                if config.debug.recreate_tasks:
                     self.delete_tasks()

        return True


    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome starting up, ESPHome integration tasks run every {self._sampling_integrations} seconds")
            await self.influx_tasks()
        except FailedInitialization as e:
            _LOGGER.error(f"run(): {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")


    async def stop(self):
        """Shutdown."""
        if self._task_gather:
            self._task_gather.cancel()


    def influx_device_integration_tasks(self, organization, sensors, periods=None) -> None:
        """Create the InfluxDB tasks to integrate devices."""

        def _integration_worker(organization, sensors, period) -> None:
            tasks_api = self._tasks_api
            bucket = self._bucket
            task_organization = organization.name
            task_base_name = 'cs_esphome.device.integration'
            try:
                for sensor in sensors:
                    location = sensor.get('location')
                    device = sensor.get('device')
                    measurement = sensor.get('measurement')
                    location_filter = '// No location' if len(location) == 0 else f'|> filter(fn: (r) => r._location == "{location}")'
                    location_map = '' if len(location) == 0 else f', _location: r._location'

                    task_name = task_base_name + '.' + device + '.' + period
                    tasks = tasks_api.find_tasks(name=task_name)
                    if tasks is None or len(tasks) == 0:
                        _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                        if period == 'today':
                            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                            flux = '\n' \
                                    f'period = "{period}"\n' \
                                    f'from(bucket: "{bucket}")\n' \
                                    f'  |> range(start: {ts})\n' \
                                    f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._device == "{device}" and r._field == "sample")\n' \
                                    f'  {location_filter}\n' \
                                    f'  |> integral(unit: 1h, column: "_value")\n' \
                                    f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                    f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                        elif period == 'month':
                            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                            flux = f'\n' \
                                    f'period = "{period}"\n' \
                                    f'from(bucket: "{bucket}")\n' \
                                    f'  |> range(start: {ts})\n' \
                                    f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "today")\n' \
                                    f'  {location_filter}\n' \
                                    f'  |> sum(column: "_value")\n' \
                                    f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                    f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                        elif period == 'year':
                            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                            flux = f'\n' \
                                    f'period = "{period}"\n' \
                                    f'from(bucket: "{bucket}")\n' \
                                    f'  |> range(start: {ts})\n' \
                                    f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "month")\n' \
                                    f'  {location_filter}\n' \
                                    f'  |> sum(column: "_value")\n' \
                                    f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                    f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                        try:
                            tasks_api.create_task_every(name=task_name, flux=flux, every=f'{self._sampling_integrations }s', organization=organization)
                            # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                        except ApiException as e:
                            body_dict = json.loads(e.body)
                            _LOGGER.error(f"ApiException during task creation in influx_integration_tasks(): {body_dict.get('message', '???')}")
                        except Exception as e:
                            _LOGGER.error(f"Unexpected exception during task creation in _integration_worker(): {e}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation in _integration_worker(): {e}")


        if periods is None:
            periods = ['today', 'month', 'year']
        try:
            for period in periods:
                _integration_worker(organization, sensors, period)
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")


    def influx_delta_wh_tasks(self, organization, periods=None) -> None:
        """."""

        def _delta_wh_worker(organization, period, range) -> None:
            tasks_api = self._tasks_api
            task_base_name = 'cs_esphome.meter'
            task_measurement = 'energy'
            task_device = 'delta_wh'

            task_name = task_base_name + '.' + task_device + '.' + period
            tasks = tasks_api.find_tasks(name=task_name)
            if tasks is None or len(tasks) == 0:
                _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
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
                    # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation in _delta_wh_worker(): {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation in _delta_wh_worker(): {e}")

        range = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        if periods is None:
            periods = ['today', 'month', 'year']
        try:
            for period in periods:
                _delta_wh_worker(organization=organization, period=period, range=range.get(period))
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")


    def influx_meter_tasks(self, organization, force=False) -> None:
        """."""
        tasks_api = self._tasks_api
        bucket = self._bucket
        task_base_name = 'cs_esphome.meter'

        task_name = task_base_name + '.meter_reading.today'
        tasks = tasks_api.find_tasks(name=task_name)
        if tasks and len(tasks) and force:
            for task in tasks:
                tasks_api.delete_task(task.id)
            tasks = tasks_api.find_tasks(name=task_name)

        if tasks is None or len(tasks) == 0:
            _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
            task_organization = organization.name
            midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp()) * 1000000000
            cron = '1 0 * * *'
            flux =  f'\n' \
                    f'from(bucket: "{bucket}")\n' \
                    f'  |> range(start: -25h)\n' \
                    f'  |> filter(fn: (r) => r._measurement == "energy" and r._field == "today")\n' \
                    f'  |> filter(fn: (r) => r._device == "meter_reading" or r._device == "delta_wh")\n' \
                    f'  |> first()\n' \
                    f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "energy", _device: "meter_reading", _field: "today", _value: r.meter_reading + (r.delta_wh * 0.001) }}))\n' \
                    f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n' \
                    f'  |> map(fn: (r) => ({{ _time: time(v: {midnight}), _measurement: "energy", _device: "meter_reading", _field: "today", _value: r._value }}))\n' \
                    f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n' \
                    f'  |> yield(name: "meter_reading")\n'

            try:
                tasks_api.create_task_cron(name=task_name, flux=flux, cron=cron, org_id=organization.id)
                _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"ApiException during task creation in influx_meter_tasks(): {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation in influx_meter_tasks(): {e}")


    async def influx_tasks(self, periods=None) -> None:
        _LOGGER.debug(f"influx_tasks(periods={periods})")
        influxdb_client = self._client

        organizations_api = influxdb_client.organizations_api()
        try:
            organizations = organizations_api.find_organizations(org=influxdb_client.org())
        except ApiException as e:
            body_dict = json.loads(e.body)
            _LOGGER.error(f"influx_tasks() can't access the InfluxDB organization: {body_dict.get('message', '???')}")
            return
        except Exception as e:
            _LOGGER.error(f"influx_tasks() can't create an InfluxDB task: unexpected exception: {e}")
            return

        self.influx_meter_tasks(organization=organizations[0])
        self.influx_delta_wh_tasks(organization=organizations[0], periods=periods)
        self.influx_device_integration_tasks(organization=organizations[0], sensors=self._sensors_by_integration, periods=periods)


    async def refresh_tasks(self, periods) -> None:
        try:
            self.delete_tasks(periods)
            await self.influx_tasks(periods)
        except ApiException as e:
            body_dict = json.loads(e.body)
            _LOGGER.error(f"refresh_tasks() can't access the InfluxDB organization: {body_dict.get('message', '???')}")
            return
        except Exception as e:
            _LOGGER.error(f"refresh_tasks() can't create an InfluxDB task: unexpected exception: {e}")
            return


    def delete_tasks(self, periods=None) -> None:
        _LOGGER.info(f"delete_tasks({periods})")
        influxdb_client = self._client
        tasks_api = influxdb_client.tasks_api()
        default_periods = ['today', 'month', 'year']
        if periods is None:
            periods = default_periods
        try:
            tasks = tasks_api.find_tasks()
            for task in tasks:
                for period in periods:
                    if task.name.endswith(period):
                        _LOGGER.info(f"'Deleting {task.name}")
                        tasks_api.delete_task(task.id)
        except Exception as e:
            _LOGGER.error(f"delete_tasks(): unexpected exception: {e}")
