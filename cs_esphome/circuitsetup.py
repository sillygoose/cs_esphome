"""Code to interface with the CircuitSetup 6-channel energy monitor using ESPHome."""

import os
import time
import asyncio
import logging
import datetime

from aioesphomeapi import SensorState

import version
from tasks import TaskManager
from esphome import ESPHomeApi

from influx import InfluxDB
from exceptions import WatchdogTimer, InfluxDBFormatError, FailedInitialization


_LOGGER = logging.getLogger('cs_esphome')


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _DEFAULT_WATCHDOG = 60
    _WATCHDOG = 0

    def __init__(self, config):
        """Create a new CircuitSetup object."""
        self._config = config
        self._task_manager = None
        self._influxdb_client = None
        self._task_gather = None
        self._esphome_api = None
        self._esphome_name = None
        self._watchdog = CircuitSetup._DEFAULT_WATCHDOG

    async def start(self) -> bool:
        """Initialize the CS/ESPHome API."""

        def _start_influxdb(config) -> bool:
            success = False
            if 'influxdb2' in config.keys():
                self._influxdb_client = InfluxDB(config)
                success = self._influxdb_client.start()
                if not success:
                    self._influxdb_client = None
            else:
                _LOGGER.error("'influxdb2' options missing from YAML configuration file")
            return success

        _LOGGER.info(f"CS/ESPHome energy collection utility {version.get_version()}, PID is {os.getpid()}")
        config = self._config
        if 'settings' in config.keys():
            if 'watchdog' in config.settings.keys():
                self._watchdog = config.settings.get('watchdog', CircuitSetup._DEFAULT_WATCHDOG)

        if not _start_influxdb(config=config):
            return False

        self._esphome_api = ESPHomeApi(config=config)
        if not await self._esphome_api.start():
            return False
        self._esphome_name = self._esphome_api.name()

        self._task_manager = TaskManager(config=config, influxdb_client=self._influxdb_client)
        if not await self._task_manager.start(by_location=self._esphome_api.sensors_by_location(), by_integration=self._esphome_api.sensors_by_integration()):
            return False

        return True

    async def run(self):
        try:
            queue = asyncio.Queue()
            self._task_gather = asyncio.gather(
                self._task_manager.run(),
                self.task_deletions(),
                self.task_esphome_sensor_post(queue),
                self.task_esphome_sensor_gather(queue),
            )
            await self._task_gather
        except FailedInitialization as e:
            _LOGGER.error(f"run(): {e}")
        except WatchdogTimer as e:
            _LOGGER.debug(f"run(): {e}")
            raise
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")

    async def stop(self):
        """Shutdown."""
        if self._task_manager:
            await self._task_manager.stop()
            self._task_manager = None

        if self._esphome_api:
            await self._esphome_api.disconnect()
            self._esphome_api = None

        if self._influxdb_client:
            self._influxdb_client.stop()
            self._influxdb_client = None

        if self._task_gather:
            self._task_gather.cancel()
            self._task_gather = None

        await asyncio.sleep(1.0)

    async def task_deletions(self) -> None:
        """Task to remove older database entries."""
        delete_api = self._influxdb_client.delete_api()
        bucket = self._influxdb_client.bucket()
        org = self._influxdb_client.org()

        pruning_tasks = []
        config = self._config
        if 'influxdb2' in config.keys():
            if 'pruning' in config.influxdb2.keys():
                for pruning_task in config.influxdb2.pruning:
                    for task in pruning_task.values():
                        name = task.get('name', None)
                        keep_last = task.get('keep_last', 30)
                        predicate = task.get('predicate', None)
                        if name and predicate:
                            new_task = {'name': name, 'predicate': predicate, 'keep_last': keep_last}
                            pruning_tasks.append(new_task)
                            _LOGGER.debug(f"Added database pruning task: {new_task}")

        while True:
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(1, 30))
            await asyncio.sleep((midnight - right_now).total_seconds())

            try:
                start = datetime.datetime(1970, 1, 1).isoformat() + 'Z'
                for task in pruning_tasks:
                    stop = datetime.datetime.combine(datetime.datetime.now() - datetime.timedelta(days=keep_last), datetime.time(0, 0)).isoformat() + 'Z'
                    predicate = task.get('predicate')
                    keep_last = task.get('keep_last')
                    delete_api.delete(start, stop, predicate, bucket=bucket, org=org)
                    _LOGGER.info(f"Pruned database '{bucket}': {predicate}, kept last {keep_last} days")
            except Exception as e:
                _LOGGER.debug(f"Unexpected exception in task_deletions(): {e}")

    async def task_esphome_sensor_post(self, queue):
        """Process the subscribed data."""
        batch_ts = 0
        batch_sensors = []
        try:
            watchdog = self._watchdog
            while True:
                try:
                    packet = queue.get_nowait()
                except asyncio.QueueEmpty:
                    _PACKET_DELAY = 0.1
                    watchdog -= _PACKET_DELAY
                    if watchdog < 0.0:
                        raise WatchdogTimer(f"Lost connection to {self._esphome_name}")
                    await asyncio.sleep(_PACKET_DELAY)
                    continue

                sensor = packet.get('sensor', None)
                state = packet.get('state', None)
                queue.task_done()
                watchdog = self._watchdog

                if sensor and state and self._influxdb_client:
                    ts = packet.get('ts', None)
                    if batch_ts != ts:
                        try:
                            self._influxdb_client.write_batch_sensors(batch_sensors=batch_sensors, timestamp=batch_ts)
                            batch_ts = ts
                            batch_sensors = [packet]
                        except InfluxDBFormatError as e:
                            _LOGGER.warning(f"{e}")
                    else:
                        batch_sensors.append(packet)
        except WatchdogTimer:
            raise
        except Exception as e:
            _LOGGER.error(f"task_esphome_sensor_post(): {e}")

    async def task_esphome_sensor_gather(self, queue):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == SensorState:
                ts = (int(time.time()) // 10) * 10
                sensor = sensors_by_key.get(state.key, None)
                if sensor:
                    queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': ts})
                    # if sensor.get('location') == 'basement':
                    #    _LOGGER.debug(f": device='{sensor.get('device')}' name='{sensor.get('sensor_name')}'  state='{state.state}'  ts='{ts}'")

        try:
            sensors_by_key = self._esphome_api.sensors_by_key()
            await self._esphome_api.subscribe_states(sensor_callback)
        except Exception as e:
            _LOGGER.error(f"task_esphome_sensor_gather(): {e}")
