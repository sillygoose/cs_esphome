"""Code to interface with the CircuitSetup 6-channel energy monitor using ESPHome."""

import os
import time
import asyncio
import logging
import datetime
import json

from dateutil.relativedelta import relativedelta

from aioesphomeapi import SensorState

import version
import query
import tasks
import esphome
import filldata
from readconfig import retrieve_options

from influx import InfluxDB
from influxdb_client.rest import ApiException
from exceptions import WatchdogTimer, InfluxDBFormatError, InfluxDBWriteError, InternalError, FailedInitialization


_LOGGER = logging.getLogger('cs_esphome')


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _INFLUX = None

    _DEFAULT_WATCHDOG = 60
    _WATCHDOG = 0

    def __init__(self, config):
        """Create a new CircuitSetup object."""
        self._config = config
        self._task_manager = None
        self._task_gather = None
        self._esphome_api = None
        self._name = None
        self._watchdog = CircuitSetup._DEFAULT_WATCHDOG


    async def start(self):
        """Initialize the CS/ESPHome API."""

        def _start_influxdb(config) -> bool:
            success = False
            if 'influxdb2' in config.keys():
                CircuitSetup._INFLUX = InfluxDB(config)
                success = CircuitSetup._INFLUX.start()
                if not success:
                    CircuitSetup._INFLUX = None
            return success

        def _check_for_meter_update(config) -> None:
            if 'meter' in config.keys():
                if 'enable_setting' in config.meter.keys():
                    enable_setting = config.meter.get('enable_setting', None)
                    if enable_setting:
                        if 'value' in config.meter.keys():
                            value = config.meter.get('value', None)
                            right_now = datetime.datetime.now()
                            midnight = datetime.datetime.combine(right_now, datetime.time(0, 0))
                            CircuitSetup._INFLUX.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'meter_reading'}], field='today', value=value, timestamp=int(midnight.timestamp()))
                        else:
                            _LOGGER.warning("Expected meter value in YAML file, no action taken")

        config = self._config
        if 'settings' in config.keys():
            if 'watchdog' in config.settings.keys():
                self._watchdog = config.settings.get('watchdog', CircuitSetup._DEFAULT_WATCHDOG)

        if not _start_influxdb(config):
            return False

        self._esphome_api = esphome.ESPHomeApi(config)
        if not await self._esphome_api.start(config):
            return False

        self._task_manager = tasks.TaskManager(config, CircuitSetup._INFLUX)
        if not await self._task_manager.start(by_location=self._esphome_api.sensors_by_location(), by_integration=self._esphome_api.sensors_by_integration()):
            return False

        _check_for_meter_update(config)
        return True


    async def run(self):
        _LOGGER.info(f"CS/ESPHome core starting up...")
        filldata.filldata(self._config, CircuitSetup._INFLUX)
        try:
            queues = {
                'sampler': asyncio.Queue(),
                'refresh': asyncio.Queue(),
            }
            self._task_gather = asyncio.gather(
                self._task_manager.run(),
                self.midnight(queues.get('refresh')),
                self.watchdog(),
                self.task_refresh(queues.get('refresh')),
                self.task_sampler(queues.get('sampler')),
                self.posting_task(queues.get('sampler')),
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

        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None

        if self._task_gather:
            self._task_gather.cancel()
            await asyncio.sleep(0.5)


    async def midnight(self, queue) -> None:
        """Task to wake up after midnight and update the solar data for the new day."""
        while True:
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(0, 0))
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
                _LOGGER.error(f"midnight() has a problem with the query: {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception in midnight(): {e}")

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


    async def posting_task(self, queue):
        """Process the subscribed data."""
        try:
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
        except Exception as e:
            _LOGGER.debug(f"posting_task(): {e}")


    async def task_sampler(self, queue):
        """Post the subscribed data."""
        def sensor_callback(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == SensorState:
                ts = (int(time.time()) // 10) * 10
                sensor = self._esphome_api.sensors_by_key().get(state.key, None)
                queue.put_nowait({'sensor': sensor, 'state': state.state, 'ts': ts})

        try:
            await self._esphome_api.subscribe_states(sensor_callback)
        except Exception as e:
            _LOGGER.debug(f"task_sampler(): {e}")
