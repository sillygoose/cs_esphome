"""InfluxDB task management for CS/ESPHome."""

import logging
import time
import json
import asyncio
import datetime

import query

from influxdb_client.rest import ApiException
from exceptions import FailedInitialization, InternalError, InfluxDBWriteError  # WatchdogTimer, InfluxDBFormatError, ,


_LOGGER = logging.getLogger('cs_esphome')


class TaskManager():
    """Class to create and manage InfluxDB tasks."""

    _DEFAULT_INTEGRATIONS = 30

    def __init__(self, config, influx_client):
        """Create a new TaskManager object."""
        self._config = config
        self._client = influx_client
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

        self._sampling_integrations = TaskManager._DEFAULT_INTEGRATIONS ###
        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_integrations = config.settings.sampling.get('integrations', TaskManager._DEFAULT_INTEGRATIONS)

        success = False
        try:
            organizations = self._organizations_api.find_organizations(org=client.org())
            success = True
        except ApiException as e:
            body_dict = json.loads(e.body)
            _LOGGER.error(f"TaskManager.start() can't access the InfluxDB organization: {body_dict.get('message', '???')}")
        except Exception as e:
            _LOGGER.error(f"TaskManager.start() can't create an InfluxDB task: unexpected exception: {e}")
        return success


    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome starting up, ESPHome integration tasks run every {self._sampling_integrations} seconds")
            queues = {
                'integrations': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self.influx_tasks(),
            )
            await self._task_gather
        except FailedInitialization as e:
            _LOGGER.error(f"run(): {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")


    async def stop(self):
        """Shutdown."""
        if self._task_gather:
            self._task_gather.cancel()


    ### device integation
    def influx_integration_tasks(self, tasks_api, organization) -> None:
        bucket = self._bucket
        task_organization = organization.name
        task_base_name = 'cs_esphome.device'
        types = {'integration': self._sensors_by_integration}
        periods = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        try:
            for period, range in periods.items():
                for type, sensors in types.items():
                    for sensor in sensors:
                        location = sensor.get('location')
                        device = sensor.get('device')
                        measurement = sensor.get('measurement')
                        location_query = '//' if len(location) == 0 else f' |> filter(fn: (r) => r._location == "{location}")'

                        task_name = task_base_name + '.' + type + '.' + device + '.' + period
                        tasks = tasks_api.find_tasks(name=task_name)
                        if tasks is None or len(tasks) == 0:
                            _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                            if period == 'today':
                                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                                flux = '\n' \
                                        f'period = "{period}"\n' \
                                        f'from(bucket: "{bucket}")\n' \
                                        f'  |> range(start: {ts})\n' \
                                        f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{device}")\n' \
                                        f'  {location_query}\n' \
                                        f'  |> integral(unit: 1h, column: "_value")\n' \
                                        f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._field, _field: period, _measurement: "energy", _value: r._value}}))\n' \
                                        f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                            elif period == 'month':
                                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                                flux = f'\n' \
                                        f'period = "{period}"\n' \
                                        f'from(bucket: "{bucket}")\n' \
                                        f'  |> range(start: {ts})\n' \
                                        f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "today")\n' \
                                        f'  {location_query}\n' \
                                        f'  |> sum(column: "_value")\n' \
                                        f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value}}))\n' \
                                        f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                            elif period == 'year':
                                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                                flux = f'\n' \
                                        f'period = "{period}"\n' \
                                        f'from(bucket: "{bucket}")\n' \
                                        f'  |> range(start: {ts})\n' \
                                        f'  |> filter(fn: (r) => r._measurement == "energy" and r._device == "{device}" and r._field == "month")\n' \
                                        f'  {location_query}\n' \
                                        f'  |> sum(column: "_value")\n' \
                                        f'  |> map(fn: (r) => ({{ _time: r._start, _device: r._device, _field: period, _measurement: "energy", _value: r._value}}))\n' \
                                        f'  |> to(bucket: "{bucket}", org: "{task_organization}")\n'
                            try:
                                tasks_api.create_task_every(name=task_name, flux=flux, every=f'{self._sampling_integrations }s', organization=organization)
                                _LOGGER.info(f"InfluxDB task '{task_name}' was successfully created")
                            except ApiException as e:
                                body_dict = json.loads(e.body)
                                _LOGGER.error(f"ApiException during task creation in influx_integration_tasks(): {body_dict.get('message', '???')}")
                            except Exception as e:
                                _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_integration_tasks(): {e}")


    def influx_delta_wh_tasks(self, tasks_api, organization) -> None:
        task_base_name = 'cs_esphome.meter'
        task_measurement = 'energy'
        task_device = 'delta_wh'

        periods = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        for period, range in periods.items():
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
                    _LOGGER.info(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation in influx_delta_wh_tasks(): {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation in influx_delta_wh_tasks(): {e}")


    async def influx_tasks(self) -> None:
        influxdb_client = self._client
        tasks_api = influxdb_client.tasks_api()
        organizations_api = influxdb_client.organizations_api()

        task_organization = influxdb_client.org()
        try:
            organizations = organizations_api.find_organizations(org=task_organization)
        except ApiException as e:
            body_dict = json.loads(e.body)
            _LOGGER.error(f"influx_tasks() can't access the InfluxDB organization: {body_dict.get('message', '???')}")
            return
        except Exception as e:
            _LOGGER.error(f"influx_tasks() can't create an InfluxDB task: unexpected exception: {e}")
            return

        self.influx_delta_wh_tasks(tasks_api=tasks_api, organization=organizations[0])
        self.influx_integration_tasks(tasks_api=tasks_api, organization=organizations[0])
