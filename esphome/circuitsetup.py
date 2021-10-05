"""Code to interface with the CircuitSetup 6-channel energy monitor."""

from dataclasses import field
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


#accuracy_decimals
def parse_sensors(yaml, entities):
    sensors_by_name = {}
    sensors_by_key = {}
    keys_by_name = dict((sensor.name, sensor.key) for sensor in entities)
    units_by_name = dict((sensor.name, sensor.unit_of_measurement) for sensor in entities)
    decimals_by_name = dict((sensor.name, sensor.accuracy_decimals) for sensor in entities)
    for entry in yaml:
        for details in entry.values():
            sensor_name = details.get('sensor_name', None)
            display_name = details.get('display_name', None)
            measurement = details.get('measurement', None)
            tag = details.get('tag', None)
            field = details.get('field', None)
            unit = units_by_name.get(sensor_name, None)
            key = keys_by_name.get(sensor_name, None)
            decimals = decimals_by_name.get(sensor_name, None)

            if key and unit:
                data = {
                    'sensor_name': sensor_name,
                    'display_name': display_name,
                    'unit': unit, 'key': key,
                    'precision': decimals,
                    'measurement': measurement,
                    'tag': tag,
                    'field': field,
                }
                sensors_by_name[sensor_name] = data
                sensors_by_key[key] = data

    return sensors_by_name, sensors_by_key


class CircuitSetup():
    """Class to describe CircuitSetup hardware."""

    _SENSOR_LOOKUP = None
    _INFLUX = None

    def __init__(self, config):
        """Create a new CS object."""
        self._config = config
        self._esphome = None
        self._name = None
        self._sensors_by_name = None
        self._sensors_by_key = None
        CircuitSetup._INFLUX = InfluxDB()

    async def start(self):
        """Initialize the CS ESPHome API."""
        config = self._config
        self._tzinfo = tz.gettz(config.site.tz)

        try:
            url = config.circuitsetup.url
            port = config.circuitsetup.port
            password = config.circuitsetup.password
            self._esphome = aioesphomeapi.APIClient(eventloop=asyncio.get_running_loop(), address=url, port=port, password=password)
            await self._esphome.connect(login=True)

            api_version = self._esphome.api_version
            _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
            return False

        try:
            device_info = await self._esphome.device_info()
            self._name = device_info.name
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}")
            _LOGGER.info(f"ESPHome version: {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing device_info(): {e}")
            return False

        try:
            entities, services = await self._esphome.list_entities_services()
            CircuitSetup._SENSOR_LOOKUP = dict((sensor.key, sensor.name) for sensor in entities)
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing '{self._name}' list_entities_services(): {e}")
            return False

        if 'influxdb2' in config.keys():
            try:
                CircuitSetup._INFLUX.start(config=config.influxdb2)
            except FailedInitialization:
                return False

        self._sensors_by_name, self._sensors_by_key = parse_sensors(yaml=config.sensors, entities=entities)
        return True

    async def run(self):
        """Run the site and wait for an event to exit."""
        def prepare(state):
            if type(state) == aioesphomeapi.SensorState:
                sensor = self._sensors_by_key.get(state.key, None)
                if sensor and CircuitSetup._INFLUX:
                    CircuitSetup._INFLUX.write_sensor(sensor=sensor, state=state.state)

        await self._esphome.subscribe_states(prepare)
        while True:
            await asyncio.sleep(2)

    async def stop(self):
        """Shutdown."""
        if self._esphome:
            await self._esphome.disconnect()
            self._esphome = None
            await asyncio.sleep(0.25)
        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None
