"""InfluxDB task management for CS/ESPHome."""

import asyncio
import logging
import json
import datetime

from influxdb_client.rest import ApiException
from utilitymeter import UtilityMeter


_LOGGER = logging.getLogger('cs_esphome')


class TaskManager():
    """Class to create and manage InfluxDB tasks."""

    _DEFAULT_SAMPLING_LOCATIONS_TODAY = 30
    _DEFAULT_SAMPLING_LOCATIONS_MONTH = 300
    _DEFAULT_SAMPLING_LOCATIONS_YEAR = 600

    _DEFAULT_SAMPLING_INTEGRATIONS_TODAY = 30
    _DEFAULT_SAMPLING_INTEGRATIONS_MONTH = 300
    _DEFAULT_SAMPLING_INTEGRATIONS_YEAR = 600

    def __init__(self, config, influxdb_client):
        """Create a new TaskManager object."""
        self._config = config
        self._influxdb_client = influxdb_client
        self._base_name = 'cs_esphome'

        self._task_gather = None
        self._organizations_api = None
        self._tasks_api = None
        self._query_api = None

        self._bucket = None
        self._delete_before = False
        self._utility_meter = UtilityMeter(self._config, self._base_name)

        self._sampling_integrations_today = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_TODAY
        self._sampling_integrations_month = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_MONTH
        self._sampling_integrations_year = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_YEAR
        self._sampling_locations_today = TaskManager._DEFAULT_SAMPLING_LOCATIONS_TODAY
        self._sampling_locations_month = TaskManager._DEFAULT_SAMPLING_LOCATIONS_MONTH
        self._sampling_locations_year = TaskManager._DEFAULT_SAMPLING_LOCATIONS_YEAR
        self._sensors_by_integration = None
        self._sensors_by_location = None

    async def start(self, by_location, by_integration) -> bool:
        """Initialize the task manager"""
        self._sensors_by_integration = by_integration
        self._sensors_by_location = by_location

        client = self._influxdb_client
        self._organizations_api = client.organizations_api()
        self._tasks_api = client.tasks_api()
        self._query_api = client.query_api()

        self._bucket = client.bucket()
        organizations = self._organizations_api.find_organizations(org=self._influxdb_client.org())
        self._organization = organizations[0]

        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                if 'integrations' in config.settings.sampling.keys():
                    self._sampling_integrations_today = config.settings.sampling.integrations.get('today', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_TODAY)
                    self._sampling_integrations_month = config.settings.sampling.integrations.get('month', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_MONTH)
                    self._sampling_integrations_year = config.settings.sampling.integrations.get('year', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_YEAR)
                if 'locations' in config.settings.sampling.keys():
                    self._sampling_locations_today = config.settings.sampling.locations.get('today', TaskManager._DEFAULT_SAMPLING_LOCATIONS_TODAY)
                    self._sampling_locations_month = config.settings.sampling.locations.get('month', TaskManager._DEFAULT_SAMPLING_LOCATIONS_MONTH)
                    self._sampling_locations_year = config.settings.sampling.locations.get('year', TaskManager._DEFAULT_SAMPLING_LOCATIONS_YEAR)

        self._utility_meter.start(self._tasks_api, self._organization, self._bucket)

        _LOGGER.info(f"CS/ESPHome Task Manager starting up, integration tasks will run every {self._sampling_integrations_today}/{self._sampling_integrations_month}/{self._sampling_integrations_year} seconds")
        if 'influxdb2' in config.keys():
            if 'recreate_tasks' in config.influxdb2.keys():
                if config.influxdb2.recreate_tasks:
                    _LOGGER.info("Task manager deleting existing InfluxDB tasks (remove or set 'recreate_tasks' to False to disable)")
                    self.delete_tasks()

        return True

    async def run(self):
        """Create the InfluxDB tasks."""
        try:
            self._task_gather = asyncio.gather(
                self.task_refresh(),
            )
            await self._task_gather
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")

    async def stop(self):
        """Shutdown the TaskManager."""
        if self._utility_meter:
            self._utility_meter.stop()
            self._utility_meter = None
        if self._task_gather:
            self._task_gather.cancel()
            self._task_gather = None

    async def task_refresh(self) -> None:
        """Update InfluxDB tasks at midnight."""
        while True:
            # Restart any InfluxDB now, today, month, and year tasks
            periods = ['now', 'today']
            right_now = datetime.datetime.now()
            if right_now.day == 1:
                periods.append('month')
            if right_now.month == 1:
                periods.append('year')

            try:
                _LOGGER.debug(f"task_refresh({periods})")
                self.delete_tasks(periods)
                await self.influx_tasks(periods)
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"task_refresh() can't access the InfluxDB organization: {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"task_refresh() can't create an InfluxDB task: unexpected exception: {e}")

            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(0, 0))
            await asyncio.sleep((midnight - right_now).total_seconds())

    async def influx_tasks(self, periods=None) -> None:
        """."""
        _LOGGER.debug(f"(periods={periods})")
        await self._utility_meter.run_tasks()
        await self.influx_location_energy_tasks()
        await self.influx_location_power_tasks()
        await self.influx_device_integration_tasks(periods=periods)

    async def influx_device_integration_tasks(self, periods=None):
        """Create the InfluxDB tasks to integrate and sum devices."""

        def _integration_worker(period):
            """Worker function to create the integrations tasks."""
            tasks_api = self._tasks_api
            bucket = self._bucket
            organization = self._organization
            sensors = self._sensors_by_integration
            try:
                for sensor in sensors:
                    location = sensor.get('location')
                    device = sensor.get('device')
                    measurement = sensor.get('measurement')
                    location_filter = '// No location' if len(location) == 0 else f'|> filter(fn: (r) => r._location == "{location}")'
                    location_map = '' if len(location) == 0 else ', _location: r._location'
                    location_name = '' if len(location) == 0 else f'.{location}'

                    task_name = self._base_name + '._device.' + device + location_name + '.' + measurement + '.' + period
                    tasks = tasks_api.find_tasks(name=task_name)
                    if self._delete_before:
                        for task in tasks:
                            _LOGGER.debug(f"'Deleting {task.name}")
                            tasks_api.delete_task(task.id)
                            tasks = None

                    if tasks is None or len(tasks) == 0:
                        _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                        if period == 'today':
                            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                            sampling = self._sampling_integrations_today
                            flux = \
                                '\n' \
                                f'period = "{period}"\n' \
                                f'from(bucket: "{bucket}")\n' \
                                f'  |> range(start: {ts})\n' \
                                f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._device == "{device}" and r._field == "sample")\n' \
                                f'  {location_filter}\n' \
                                f'  |> integral(unit: 1h, column: "_value")\n' \
                                f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'
                        elif period == 'month':
                            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                            sampling = self._sampling_integrations_month
                            flux = \
                                f'\n' \
                                f'period = "{period}"\n' \
                                f'from(bucket: "{bucket}")\n' \
                                f'  |> range(start: {ts})\n' \
                                f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "today")\n' \
                                f'  {location_filter}\n' \
                                f'  |> sum(column: "_value")\n' \
                                f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'
                        elif period == 'year':
                            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                            sampling = self._sampling_integrations_year
                            flux = \
                                f'\n' \
                                f'period = "{period}"\n' \
                                f'from(bucket: "{bucket}")\n' \
                                f'  |> range(start: {ts})\n' \
                                f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "month")\n' \
                                f'  {location_filter}\n' \
                                f'  |> sum(column: "_value")\n' \
                                f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value{location_map} }}))\n' \
                                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'
                        try:
                            tasks_api.create_task_every(name=task_name, flux=flux, every=f'{sampling}s', organization=organization)
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
                if period in ['today', 'month', 'year']:
                    _integration_worker(period)
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")

    async def influx_location_power_tasks(self) -> None:
        """Creates the tasks that sums up power by location."""
        measurement = 'power'
        tag_key = '_location'
        period = 'now'

        bucket = self._bucket
        tasks_api = self._tasks_api
        organization = self._organization

        for location, sensors in self._sensors_by_location.items():
            task_name = self._base_name + '.' + tag_key + '.' + location + '.' + measurement + '.' + period
            tasks = tasks_api.find_tasks(name=task_name)
            if self._delete_before:
                for task in tasks:
                    _LOGGER.debug(f"'Deleting {task.name}")
                    tasks_api.delete_task(task.id)
                    tasks = None

            if tasks is None or len(tasks) == 0:
                _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                flux = \
                    f'\n' \
                    f'from(bucket: "{bucket}")\n' \
                    f'  |> range(start: {ts})\n' \
                    f'  |> last()\n' \
                    f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r.{tag_key} == "{location}" and r._field == "sample")\n' \
                    f'  |> drop(columns: ["_device"])\n' \
                    f'  |> sum(column: "_value")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._start, _measurement: "{measurement}", {tag_key}: r.{tag_key} , _field: "{period}", _value: r._value }}))\n' \
                    f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'

                try:
                    tasks_api.create_task_every(name=task_name, flux=flux, every=f'{self._sampling_locations_today}s', organization=organization)
                    # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation in influx_meter_tasks(): {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation in influx_meter_tasks(): {e}")

    async def influx_location_energy_tasks(self) -> None:
        """Creates the tasks that sums up location energy."""
        measurement = 'energy'
        tag_key = '_location'

        bucket = self._bucket
        tasks_api = self._tasks_api
        organization = self._organization

        for period in ['today', 'month', 'year']:
            for location, sensors in self._sensors_by_location.items():
                task_name = self._base_name + '.' + tag_key + '.' + location + '.' + measurement + '.' + period
                tasks = tasks_api.find_tasks(name=task_name)
                if self._delete_before:
                    for task in tasks:
                        _LOGGER.debug(f"'Deleting {task.name}")
                        tasks_api.delete_task(task.id)
                        tasks = None

                if tasks is None or len(tasks) == 0:
                    _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                    if period == 'today':
                        ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                        sampling = self._sampling_locations_today
                    elif period == 'month':
                        ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                        sampling = self._sampling_locations_month
                    elif period == 'year':
                        ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                        sampling = self._sampling_locations_year

                    flux = f'\n' \
                        f'from(bucket: "{bucket}")\n' \
                        f'  |> range(start: {ts})\n' \
                        f'  |> last()\n' \
                        f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r.{tag_key} == "{location}" and r._field == "{period}" and exists r._device)\n' \
                        f'  |> drop(columns: ["_device"])\n' \
                        f'  |> sum(column: "_value")\n' \
                        f'  |> map(fn: (r) => ({{ _time: r._start, _measurement: "{measurement}", {tag_key}: r.{tag_key} , _field: "{period}", _value: r._value }}))\n' \
                        f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'

                    try:
                        tasks_api.create_task_every(name=task_name, flux=flux, every=f'{sampling}s', organization=organization)
                        # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                    except ApiException as e:
                        body_dict = json.loads(e.body)
                        _LOGGER.error(f"ApiException during task creation in influx_meter_tasks(): {body_dict.get('message', '???')}")
                    except Exception as e:
                        _LOGGER.error(f"Unexpected exception during task creation in influx_meter_tasks(): {e}")

    def delete_tasks(self, periods=None) -> None:
        """."""
        _LOGGER.debug("delete_tasks()")
        tasks_api = self._influxdb_client.tasks_api()
        try:
            tasks = tasks_api.find_tasks()
            if periods is None:
                for task in tasks:
                    if task.name.startswith(self._base_name):
                        _LOGGER.debug(f"'Deleting {task.name}")
                        tasks_api.delete_task(task.id)
            else:
                for period in periods:
                    for task in tasks:
                        if task.name.endswith('.' + period):
                            _LOGGER.debug(f"'Deleting {task.name}")
                            tasks_api.delete_task(task.id)

        except Exception as e:
            _LOGGER.error(f"delete_tasks(): unexpected exception: {e}")
