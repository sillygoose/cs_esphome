"""Code to interface with the CircuitSetup 6-channel energy monitor using ESPHome."""

import os
import time
import asyncio
import logging
import datetime

from dateutil.relativedelta import relativedelta

from aioesphomeapi import SensorState
from influxdb_client.domain.bucket import Bucket

import version
import query
import tasks
import esphome
import filldata
import electricmeter
from readconfig import retrieve_options

from influx import InfluxDB
from influxdb_client.rest import ApiException
from exceptions import WatchdogTimer, InfluxDBFormatError, InfluxDBWriteError, InternalError, FailedInitialization


_LOGGER = logging.getLogger('cs_esphome')


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _DEFAULT_WATCHDOG = 60
    _WATCHDOG = 0

    def __init__(self, config):
        """Create a new CircuitSetup object."""
        self._config = config
        self._task_manager = None
        self._query_manager = None
        self._influxdb_client = None
        self._task_gather = None
        self._esphome_api = None
        self._electric_meter = None
        self._name = None
        self._watchdog = CircuitSetup._DEFAULT_WATCHDOG


    async def start(self):
        """Initialize the CS/ESPHome API."""

        def _start_influxdb(config) -> bool:
            success = False
            if 'influxdb2' in config.keys():
                self._influxdb_client = InfluxDB(config)
                success = self._influxdb_client.start()
                if not success:
                    self._influxdb_client = None
            return success


        config = self._config
        if 'settings' in config.keys():
            if 'watchdog' in config.settings.keys():
                self._watchdog = config.settings.get('watchdog', CircuitSetup._DEFAULT_WATCHDOG)

        if not _start_influxdb(config=config):
            return False

        self._esphome_api = esphome.ESPHomeApi(config=config)
        if not await self._esphome_api.start():
            return False
        self._name = self._esphome_api.name()

        self._electric_meter = electricmeter.ElectricMeter(config=config, influxdb_client=self._influxdb_client)
        if not self._electric_meter.start():
            return False

        self._task_manager = tasks.TaskManager(config=config, influxdb_client=self._influxdb_client)
        if not await self._task_manager.start(by_location=self._esphome_api.sensors_by_location(), by_integration=self._esphome_api.sensors_by_integration()):
            return False

        self._query_manager = query.QueryManager(config=config, influxdb_client=self._influxdb_client)
        if not await self._query_manager.start(locations=self._esphome_api.sensors_by_location()):
            return False

        return True


    async def run(self):
        filldata.filldata(self._config, self._influxdb_client)
        try:
            queues = {
                'deletions': asyncio.Queue(),
                'esphome': asyncio.Queue(),
                'refresh': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self._task_manager.run(),
                self._query_manager.run(),
                self.watchdog(),
                self.midnight(queues.get('refresh'), queues.get('deletions')),
                self.task_deletions(queues.get('deletions')),
                self.task_refresh(queues.get('refresh')),
                self.task_esphome_sensor_post(queues.get('esphome')),
                self.task_esphome_sensor_gather(queues.get('esphome')),
            )
            await self._task_gather
        except FailedInitialization as e:
            _LOGGER.error(f"run(): {e}")
        except WatchdogTimer as e:
            _LOGGER.error(f"run(): {e}")
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
            # await asyncio.sleep(0.5)

        if self._influxdb_client:
            self._influxdb_client.stop()
            self._influxdb_client = None

        if self._task_gather:
            self._task_gather.cancel()
            await asyncio.sleep(0.5)


    async def midnight(self, refresh_queue, deletion_queue) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            _LOGGER.info(f"CS/ESPHome energy collection utility {version.get_version()}, PID is {os.getpid()}")
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(0, 0))
            await asyncio.sleep((midnight - right_now).total_seconds())

            # Restart any InfluxDB today, month, and year tasks
            periods = ['today']
            right_now = datetime.datetime.now()
            if right_now.day == 1:
                periods.append('month')
            if right_now.month == 1:
                periods.append('year')
            refresh_queue.put_nowait(periods)
            _LOGGER.info(f"Posted event to the refresh queue: {periods}")

            deletion_queue.put_nowait(False)
            _LOGGER.info(f"Posted event to the deletion queue: {False}")


    async def task_deletions(self, deletion_queue) -> None:
        """Task to remove old database entries."""

        _PREDICATES = [
            {'name': 'power_factor', 'predicate': '_measurement="power_factor"', 'keep_last': 1},
            {'name': 'voltage', 'predicate': '_measurement="voltage"', 'keep_last': 3},
            {'name': '_vehicles', 'predicate': '_measurement="energy AND _device="_vehicles"', 'keep_last': 0},
        ]

        delete_api = self._influxdb_client.delete_api()
        bucket = self._influxdb_client.bucket()
        org = self._influxdb_client.org()
        start = datetime.datetime(1970, 1, 1).isoformat() + 'Z'
        while True:
            predicate = await deletion_queue.get()
            deletion_queue.task_done()
            _LOGGER.info(f"task_deletions(queue): {predicate}")

            await asyncio.sleep(60 * 60 * 4)
            try:
                for predicate in _PREDICATES:
                    keep_last = predicate.get('keep_last', 3)
                    predicate = predicate.get('predicate', None)
                    if predicate:
                        stop = datetime.datetime.combine(datetime.datetime.now() - datetime.timedelta(days=keep_last), datetime.time(0, 0)).isoformat() + 'Z'
                        delete_api.delete(start, stop, predicate, bucket=bucket, org=org)
                        _LOGGER.info(f"Pruned database '{bucket}': {predicate}, kept last {keep_last} days")
            except Exception as e:
                _LOGGER.debug(f"Unexpected exception in task_deletions(): {e}")


    def _deletions(self, predicate) -> None:
        """Remove old database entries with your predicate."""
        delete_api = self._influxdb_client.delete_api()
        bucket = self._influxdb_client.bucket()
        org = self._influxdb_client.org()
        start = datetime.datetime(1970, 1, 1).isoformat() + 'Z'
        _LOGGER.info(f"_deletions(queue): {predicate}")
        try:
            stop = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).isoformat() + 'Z'
            delete_api.delete(start, stop, predicate, bucket=bucket, org=org)
            _LOGGER.info(f"Pruned database '{bucket}': {predicate}")
        except Exception as e:
            _LOGGER.debug(f"Unexpected exception in _deletions(): {e}")


    async def watchdog(self):
        """Check that we are connected to the CircuitSetup hardware."""
        try:
            saved_watchdog = CircuitSetup._WATCHDOG
            while True:
                await asyncio.sleep(self._watchdog)
                current_watchdog = CircuitSetup._WATCHDOG
                if saved_watchdog == current_watchdog:
                    raise WatchdogTimer(f"Lost connection to {self._name}")
                saved_watchdog = current_watchdog
        except Exception as e:
            _LOGGER.debug(f"watchdog(): {e}")


    async def task_refresh(self, queue):
        """Remove old tasks and replace with new ones."""
        try:
            while True:
                periods = await queue.get()
                queue.task_done()
                _LOGGER.info(f"task_refresh(queue): {periods}")
                await self._task_manager.refresh_tasks(periods=periods)
        except Exception as e:
            _LOGGER.debug(f"task_refresh(): {e}")


    async def task_esphome_sensor_post(self, queue):
        """Process the subscribed data."""
        try:
            while True:
                packet = await queue.get()
                sensor = packet.get('sensor', None)
                state = packet.get('state', None)
                queue.task_done()
                if sensor and state and self._influxdb_client:
                    ts = packet.get('ts', None)
                    try:
                        self._influxdb_client.write_sensor(sensor=sensor, state=state, timestamp=ts)
                    except InfluxDBFormatError as e:
                        _LOGGER.warning(f"{e}")
        except Exception as e:
            _LOGGER.debug(f"posting_task(): {e}")


    async def task_esphome_sensor_gather(self, queue):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == SensorState:
                ts = (int(time.time()) // 10) * 10
                sensor = sensors_by_key.get(state.key, None)
                if sensor:
                    queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': ts})
                    #if sensor.get('location') == 'basement':
                    #    _LOGGER.debug(f": device='{sensor.get('device')}' name='{sensor.get('sensor_name')}'  state='{state.state}'  ts='{ts}'")

        try:
            sensors_by_key = self._esphome_api.sensors_by_key()
            await self._esphome_api.subscribe_states(sensor_callback)
        except Exception as e:
            _LOGGER.debug(f"task_sampler(): {e}")
