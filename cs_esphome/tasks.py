"""InfluxDB task management for CS/ESPHome."""

import logging
import json
import datetime

from influxdb_client.rest import ApiException
# from exceptions import   # WatchdogTimer, InfluxDBFormatError, ,


_LOGGER = logging.getLogger('cs_esphome')


class TaskManager():
    """Class to create and manage InfluxDB tasks."""

    _DEFAULT_SAMPLING_LOCATIONS_TODAY = 16
    _DEFAULT_SAMPLING_LOCATIONS_MONTH = 301
    _DEFAULT_SAMPLING_LOCATIONS_YEAR = 602
    _DEFAULT_SAMPLING_INTEGRATIONS_TODAY = 60
    _DEFAULT_SAMPLING_INTEGRATIONS_MONTH = 300
    _DEFAULT_SAMPLING_INTEGRATIONS_YEAR = 600
    _DEFAULT_SAMPLING_DELTA_WH = 300

    def __init__(self, config, influxdb_client):
        """Create a new TaskManager object."""
        self._config = config
        self._client = influxdb_client
        self._task_gather = None
        self._organizations_api = None
        self._tasks_api = None
        self._query_api = None
        self._bucket = None
        self._sampling_integrations_today = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_TODAY
        self._sampling_integrations_month = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_MONTH
        self._sampling_integrations_year = TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_YEAR
        self._sampling_delta_wh = TaskManager._DEFAULT_SAMPLING_DELTA_WH
        self._sampling_locations_today = TaskManager._DEFAULT_SAMPLING_LOCATIONS_TODAY
        self._sampling_locations_month = TaskManager._DEFAULT_SAMPLING_LOCATIONS_MONTH
        self._sampling_locations_year = TaskManager._DEFAULT_SAMPLING_LOCATIONS_YEAR
        self._sensors_by_integration = None
        self._sensors_by_location = None
        self._base_name = 'cs_esphome'


    async def start(self, by_location, by_integration) -> bool:
        """Initialize the task manager"""

        self._sensors_by_integration = by_integration
        self._sensors_by_location = by_location
        client = self._client

        self._organizations_api = client.organizations_api()
        self._tasks_api = client.tasks_api()
        self._query_api = client.query_api()
        self._bucket = client.bucket()

        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_delta_wh = config.settings.sampling.get('delta_wh', TaskManager._DEFAULT_SAMPLING_DELTA_WH)
                if 'integrations' in config.settings.sampling.keys():
                    self._sampling_integrations_today = config.settings.sampling.integrations.get('today', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_TODAY)
                    self._sampling_integrations_month = config.settings.sampling.integrations.get('month', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_MONTH)
                    self._sampling_integrations_year = config.settings.sampling.integrations.get('year', TaskManager._DEFAULT_SAMPLING_INTEGRATIONS_YEAR)
                if 'locations' in config.settings.sampling.keys():
                    self._sampling_locations_today = config.settings.sampling.locations.get('today', TaskManager._DEFAULT_SAMPLING_LOCATIONS_TODAY)
                    self._sampling_locations_month = config.settings.sampling.locations.get('month', TaskManager._DEFAULT_SAMPLING_LOCATIONS_MONTH)
                    self._sampling_locations_year = config.settings.sampling.locations.get('year', TaskManager._DEFAULT_SAMPLING_LOCATIONS_YEAR)

        #if 'debug' in config.keys():
        #    if 'recreate_tasks' in config.debug.keys():
        #        if config.debug.recreate_tasks:
        self.delete_tasks()

        return True


    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome Task Manager starting up, integration tasks will run every {self._sampling_integrations_today}/{self._sampling_integrations_month}/{self._sampling_integrations_year} seconds")
            await self.influx_tasks()
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
            try:
                for sensor in sensors:
                    location = sensor.get('location')
                    device = sensor.get('device')
                    measurement = sensor.get('measurement')
                    location_filter = '// No location' if len(location) == 0 else f'|> filter(fn: (r) => r._location == "{location}")'
                    location_map = '' if len(location) == 0 else f', _location: r._location'
                    location_name = '' if len(location) == 0 else f'.{location}'

                    task_name = self._base_name + '._device.'+ device + location_name+  '.' + measurement + '.' + period
                    tasks = tasks_api.find_tasks(name=task_name)
                    if tasks is None or len(tasks) == 0:
                        _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                        if period == 'today':
                            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                            sampling = self._sampling_integrations_today
                            flux = '\n' \
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
                            flux = f'\n' \
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
                            flux = f'\n' \
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
                _integration_worker(organization, sensors, period)
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")


    def influx_delta_wh_tasks(self, organization, periods=None) -> None:
        """These tasks calculate the change in Wh during a given period."""

        def _delta_wh_worker(organization, period, range) -> None:
            measurement = 'energy'
            output_key = '_meter'
            output_value = 'delta_wh'
            task_name = self._base_name + '.' + output_key + '.' + output_value + '.' + measurement + '.' + period

            tasks_api = self._tasks_api
            tasks = tasks_api.find_tasks(name=task_name)
            for task in tasks:
                if task.name.endswith(period):
                    _LOGGER.debug(f"'Deleting {task.name}")
                    tasks_api.delete_task(task.id)
                    tasks = None

            if tasks is None or len(tasks) == 0:
                _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                flux =  '\n' \
                        f'production_{period} = from(bucket: "multisma2")\n' \
                        f'  |> range(start: {range})\n' \
                        f'  |> filter(fn: (r) => r._measurement == "production" and r._field == "{period}" and r._inverter == "site")\n' \
                        f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "energy", {output_key}: r._inverter, _field: r._field, _value: r._value * 1000.0 }}))\n' \
                        f'  |> yield(name: "production_{period}")\n' \
                        f'\n' \
                        f'consumption_{period} = from(bucket: "cs24")\n' \
                        f'  |> range(start: {range})\n' \
                        f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._device == "line" and r._field == "{period}")\n' \
                        f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {output_key}: r._device, _field: r._field, _value: r._value }}))\n' \
                        f'  |> yield(name: "consumption_{period}")\n' \
                        f'\n' \
                        f'union(tables: [production_{period}, consumption_{period}])\n' \
                        f'  |> pivot(rowKey:["_time"], columnKey: ["_meter"], valueColumn: "_value")\n' \
                        f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {output_key}: "{output_value}", _field: "{period}", _value: r.line - r.site }}))\n' \
                        f'  |> to(bucket: "cs24", org: "{organization.name}")\n'

                try:
                    tasks_api.create_task_every(name=task_name, flux=flux, every=f'{self._sampling_delta_wh}s', organization=organization)
                    # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation in _delta_wh_worker(): {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation in _delta_wh_worker(): {e}")

        range = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        if periods is None:
            periods = ['today']
        try:
            for period in periods:
                _delta_wh_worker(organization=organization, period=period, range=range.get(period))
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")


    def influx_meter_tasks(self, organization, force=False) -> None:
        """Creates the cron task that updates the meter reading at midnight."""
        measurement = 'energy'
        tag_key = '_meter'
        output_value = 'reading'
        period = "today"
        task_name = self._base_name + '.' + tag_key + '.' + output_value + '.' + measurement + '.' + period

        bucket = self._bucket
        tasks_api = self._tasks_api
        tasks = tasks_api.find_tasks(name=task_name)
        if tasks and len(tasks) and force:
            for task in tasks:
                tasks_api.delete_task(task.id)
            tasks = tasks_api.find_tasks(name=task_name)

        if tasks is None or len(tasks) == 0:
            _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(0, 0))
            next_midnight = int(midnight.timestamp()) * 1000000000
            cron = '59 23 * * *'
            flux =  f'\n' \
                    f'from(bucket: "{bucket}")\n' \
                    f'  |> range(start: -1d)\n' \
                    f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{period}" and exists r.{tag_key})\n' \
                    f'  |> pivot(rowKey:["_time"], columnKey: ["{tag_key}"], valueColumn: "_value")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {tag_key}: "{output_value}", _field: "{period}", _value: r.{output_value} + (r.delta_wh * 0.001) }}))\n' \
                    f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n' \
                    f'  |> map(fn: (r) => ({{ _time: time(v: {next_midnight}), _measurement: "{measurement}", {tag_key}: "{output_value}", _field: "{period}", _value: r._value }}))\n' \
                    f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'

            try:
                tasks_api.create_task_cron(name=task_name, flux=flux, cron=cron, org_id=organization.id)
                # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"ApiException during task creation in influx_meter_tasks(): {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation in influx_meter_tasks(): {e}")


    def influx_location_power_tasks(self, organization) -> None:
        """Creates the tasks that sums up location power."""
        measurement = 'power'
        tag_key = '_location'
        period = "now"

        bucket = self._bucket
        tasks_api = self._tasks_api

        for location, sensors in self._sensors_by_location.items():
            task_name = self._base_name + '.' + tag_key + '.' + location + '.' + measurement + '.' + period
            tasks = tasks_api.find_tasks(name=task_name)
            if tasks is None or len(tasks) == 0:
                _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                flux =  f'\n' \
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


    def influx_location_energy_tasks(self, organization) -> None:
        """Creates the tasks that sums up location energy."""
        measurement = 'energy'
        tag_key = '_location'

        bucket = self._bucket
        tasks_api = self._tasks_api
        for period in ['today', 'month', 'year']:
            for location, sensors in self._sensors_by_location.items():
                task_name = self._base_name + '.' + tag_key + '.' + location + '.' + measurement + '.' + period
                tasks = tasks_api.find_tasks(name=task_name)
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
                    else:
                        pass

                    flux =  f'\n' \
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

        self.influx_location_energy_tasks(organization=organizations[0])
        self.influx_location_power_tasks(organization=organizations[0])
        self.influx_meter_tasks(organization=organizations[0])
        self.influx_delta_wh_tasks(organization=organizations[0])
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
        _LOGGER.debug(f"delete_tasks()")
        influxdb_client = self._client
        tasks_api = influxdb_client.tasks_api()
        try:
            tasks = tasks_api.find_tasks()
            if periods is None:
                for task in tasks:
                    if task.name.startswith(self._base_name):
                        _LOGGER.debug(f"'Deleting {task.name}")
                        tasks_api.delete_task(task.id)
            else:
                for period in periods:
                    endswith = period
                    for task in tasks:
                        if task.name.endswith(endswith):
                            _LOGGER.debug(f"'Deleting {task.name}")
                            tasks_api.delete_task(task.id)

        except Exception as e:
            _LOGGER.error(f"delete_tasks(): unexpected exception: {e}")
