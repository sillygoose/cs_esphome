"""InfluxDB task management for CS/ESPHome."""

import logging
import time
import json
import asyncio

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

        self._sampling_integrations = TaskManager._DEFAULT_INTEGRATIONS
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
            _LOGGER.info(f"CS/ESPHome starting up, ESPHome sensors processed every {self._sampling_integrations} seconds")
            queues = {
                'integrations': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self.task_integrations(queues.get('integrations')),
                self.scheduler(queues),
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
        influxdb_client = self._client
        query_api = self._query_api
        bucket = self._bucket
        while True:
            timestamp = await queue.get()
            queue.task_done()

            _LOGGER.debug(f"task_integrations(queue)")
            try:
                today = query.integrate_locations(query_api, bucket, self._sensors_by_location, 'today')
                today += query.integrate_devices(query_api, bucket, self._sensors_by_integration, 'today')
                influxdb_client.write_points(today)

                month = query.integrate_locations(query_api, bucket, self._sensors_by_location, 'month')
                month += query.integrate_devices(query_api, bucket, self._sensors_by_integration, 'month')
                influxdb_client.write_points(month)

                year = query.integrate_locations(query_api, bucket, self._sensors_by_location, 'year')
                year += query.integrate_devices(query_api, bucket, self._sensors_by_integration, 'year')
                influxdb_client.write_points(year)
            except InfluxDBWriteError as e:
                _LOGGER.warning(f"{e}")
            except InternalError as e:
                _LOGGER.error(f"Internal error detected in task_integrations(): {e}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception detected in task_integrations(): {e}")


