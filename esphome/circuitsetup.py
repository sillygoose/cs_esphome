"""Code to interface with the CircuitSetup 6-channel energy monitor."""

import os
import asyncio
import datetime
import time
import logging
from dateutil import tz
import aioesphomeapi

import version

from influx import InfluxDB

from exceptions import FailedInitialization


_LOGGER = logging.getLogger('esphome')


class CS24():
    """Class to describe a CS hardware."""

    def __init__(self, loop, config):
        """Create a new CS object."""
        self._config = config
        self._loop = loop
        self._esphome = None
        self._sensor_by_keys = None
        self._influx = InfluxDB()

    async def start(self):
        """Initialize the PVSite object."""
        config = self._config
        #self._tzinfo = tz.gettz(config.tz)

        try:
            self._esphome = aioesphomeapi.APIClient(eventloop=self._loop, address="cs24.local", port=6053, password="")
            await self._esphome.connect(login=True)

            api_version = self._esphome.api_version
            _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
            return False

        try:
            device_info = await self._esphome.device_info()
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}")
            _LOGGER.info(f"ESPHome version: {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing device_info(): {e}")
            return False

        try:
            sensors, services = await self._esphome.list_entities_services()
            self._sensor_by_keys = dict((sensor.key, sensor.name) for sensor in sensors)
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing list_entities_services(): {e}")
            return False

        if 'influxdb2' in config.keys():
            try:
                self._influx.start(config=config.influxdb2)
            except FailedInitialization:
                return False
        return True

    async def run(self):
        """Run the site and wait for an event to exit."""
        def cb(state):
            if type(state) == aioesphomeapi.SensorState:
                _LOGGER.info(f"working...")
                #_LOGGER.info(f"{sensor_by_keys[state.key]}: {state.state}")

        await self._esphome.subscribe_states(cb)
        while True:
            await asyncio.sleep(5)
            _LOGGER.info(f"waiting...")

    async def stop(self):
        """Shutdown."""
        if self._esphome:
            await self._esphome.disconnect()
            self._esphome = None
        self._influx.stop()
