"""Code to interface with the CircuitSetup 6-channel energy monitor."""

import time
import asyncio
import logging
import aioesphomeapi
from aioesphomeapi.core import SocketAPIError, InvalidAuthAPIError

from influx import InfluxDB
from exceptions import FailedInitialization, WatchdogTimer

_LOGGER = logging.getLogger('esphome')


def parse_sensors(yaml, entities):
    sensors_by_name = {}
    sensors_by_key = {}
    keys_by_name = dict((sensor.name, sensor.key) for sensor in entities)
    units_by_name = dict((sensor.name, sensor.unit_of_measurement) for sensor in entities)
    decimals_by_name = dict((sensor.name, sensor.accuracy_decimals) for sensor in entities)
    for entry in yaml:
        for details in entry.values():
            enable = details.get('enable', True)
            sensor_name = details.get('sensor_name', None)
            key = keys_by_name.get(sensor_name, None)
            if key and enable:
                now = time.time()
                data = {
                    'sensor_name': details.get('sensor_name', None),
                    'display_name': details.get('display_name', None),
                    'unit': units_by_name.get(sensor_name, None),
                    'key': keys_by_name.get(sensor_name, None),
                    'precision': decimals_by_name.get(sensor_name, None),
                    'measurement': details.get('measurement', None),
                    'tag': details.get('tag', None),
                    'field': details.get('field', None),
                    'last_sample': now,
                    'today': 0.0,
                    'month': 0.0,
                    'year': 0.0,
                }
                sensors_by_name[sensor_name] = data
                sensors_by_key[key] = data

    return sensors_by_name, sensors_by_key


class CircuitSetup():
    """Class to describe the CircuitSetup ESPHome API."""

    _INFLUX = None
    _DEFAULT_ESPHOME_API_PORT = 6053
    _DEFAULT_ESPHOME_API_PASSWORD = ''

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
        try:
            url = config.circuitsetup.url
            port = config.circuitsetup.get('port', CircuitSetup._DEFAULT_ESPHOME_API_PORT)
            password = config.circuitsetup.get('password', CircuitSetup._DEFAULT_ESPHOME_API_PASSWORD)
            self._esphome = aioesphomeapi.APIClient(eventloop=asyncio.get_running_loop(), address=url, port=port, password=password)
            await self._esphome.connect(login=True)
        except SocketAPIError as e:
            _LOGGER.error(f"{e}")
            return False
        except InvalidAuthAPIError as e:
            _LOGGER.error(f"ESPHome login failed: {e}")
            return False
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
            return False

        try:
            api_version = self._esphome.api_version
            _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")

            device_info = await self._esphome.device_info()
            self._name = device_info.name
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}")
            _LOGGER.info(f"ESPHome version: {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing version and/or device_info: {e}")
            return False

        try:
            entities, services = await self._esphome.list_entities_services()
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing '{self._name}' list_entities_services(): {e}")
            return False

        if 'influxdb2' in config.keys():
            if not CircuitSetup._INFLUX.start(config=config.influxdb2):
                return False

        self._sensors_by_name, self._sensors_by_key = parse_sensors(yaml=config.sensors, entities=entities)
        return True

    _WATCHDOG = 0
    async def watchdog(self):
        """Check that we are connected to the CircuitSetup hardware."""
        saved_watchdog = CircuitSetup._WATCHDOG
        while True:
            await asyncio.sleep(60)
            current_watchdog = CircuitSetup._WATCHDOG
            if saved_watchdog == current_watchdog:
                raise WatchdogTimer(f"Lost connection to {self._name}")
            saved_watchdog = current_watchdog

    async def run(self):
        """Run and process the subscribed data."""
        def prepare(state):
            CircuitSetup._WATCHDOG += 1
            if type(state) == aioesphomeapi.SensorState:
                sensor = self._sensors_by_key.get(state.key, None)
                if sensor:
                    now = time.time()
                    ts = int(now)
                    if sensor.get('integralx', False):
                        previous = sensor.get('last_sample', None)
                        delta = now - previous
                        integral = state.state * delta
                        sensor['last_sample'] = now
                        sensor['today'] += integral
                        sensor['month'] += integral
                        sensor['year'] += integral
                    if CircuitSetup._INFLUX:
                        CircuitSetup._INFLUX.write_sensor(sensor=sensor, state=state.state, timestamp=ts)
                        #CircuitSetup._INFLUX.write_integral(sensor=sensor, timestamp=ts)
        try:
            await asyncio.gather(
                self._esphome.subscribe_states(prepare),
                self.watchdog(),
            )
        except WatchdogTimer as e:
            _LOGGER.error(f"'{e}', exiting")
            self.stop()

    async def stop(self):
        """Shutdown."""
        if self._esphome:
            await self._esphome.disconnect()
            self._esphome = None
            await asyncio.sleep(0.25)
        if CircuitSetup._INFLUX:
            CircuitSetup._INFLUX.stop()
            CircuitSetup._INFLUX = None
