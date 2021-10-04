"""Code to interface with the CircuitSetup 6-channel energy monitor."""

import os
import asyncio
import datetime
import time
import logging
from dateutil import tz

import version

from influx import InfluxDB

from exceptions import FailedInitialization


_LOGGER = logging.getLogger('esphome')


class CS24():
    """Class to describe a CS hardware."""

    def __init__(self, session, config):
        """Create a new PVSite object."""
        self._config = config
        self._tzinfo = None
        self._influx = InfluxDB()

    async def start(self):
        """Initialize the PVSite object."""
        config = self._config
        self._tzinfo = tz.gettz(config.tz)

        if 'influxdb2' in config.keys():
            try:
                self._influx.start(config=config.influxdb2)
            except FailedInitialization:
                return False
        return True

    async def run(self):
        """Run the site and wait for an event to exit."""

    async def stop(self):
        """Shutdown."""
        self._influx.stop()
