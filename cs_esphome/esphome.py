"""ESPHome API class."""

import logging

import aioesphomeapi
from aioesphomeapi.core import SocketAPIError, InvalidAuthAPIError

import sensors


_LOGGER = logging.getLogger('cs_esphome')


class ESPHomeApi():
    """Class to manage the ESPHome API."""

    _DEFAULT_ESPHOME_API_PORT = 6053
    _DEFAULT_ESPHOME_API_PASSWORD = ''

    def __init__(self, config):
        """Create a new ESPHomeApi object."""
        self._config = config
        self._client = None
        self._sensors_by_name = None
        self._sensors_by_key = None
        self._sensor_integrate = None
        self._sensor_locations = None


    async def start(self):
        """."""
        success = False
        config = self._config
        try:
            url = config.circuitsetup.url
            port = config.circuitsetup.get('port', ESPHomeApi._DEFAULT_ESPHOME_API_PORT)
            password = config.circuitsetup.get('password', ESPHomeApi._DEFAULT_ESPHOME_API_PASSWORD)
            self._client = aioesphomeapi.APIClient(address=url, port=port, password=password)
            await self._client.connect(login=True)
            success = True
        except SocketAPIError as e:
            _LOGGER.error(f"{e}")
        except InvalidAuthAPIError as e:
            _LOGGER.error(f"ESPHome login failed: {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
        finally:
            if not success:
                self._client = None
                return False

        try:
            api_version = self._client.api_version
            _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")

            device_info = await self._client.device_info()
            self._name = device_info.name
            _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}, version {device_info.esphome_version} built on {device_info.compilation_time}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing version and/or device_info: {e}")
            return False

        try:
            entities, services = await self._client.list_entities_services()
            sample_period = None
            extra = ''
            for sensor in entities:
                if sensor.name == 'cs24_sampling':
                    sample_period = sensor.accuracy_decimals
                    extra = f', ESPHome reports sampling sensors every {sample_period} seconds'
                    break
            _LOGGER.info(f"CS/ESPHome core started{extra}")

        except Exception as e:
            _LOGGER.error(f"Unexpected exception accessing '{self._name}' list_entities_services(): {e}")
            return False

        self._sensors_by_name, self._sensors_by_key = sensors.parse_sensors(yaml=config.sensors, entities=entities)
        self._sensors_by_location = sensors.parse_by_location(self._sensors_by_name)
        self._sensors_by_integration = sensors.parse_by_integration(self._sensors_by_name)
        return True


    async def disconnect(self):
        await self._client.disconnect()


    async def subscribe_states(self, sensor_callback):
        await self._client.subscribe_states(sensor_callback)


    def sensors_by_name(self):
        return self._sensors_by_name


    def sensors_by_key(self):
        return self._sensors_by_key


    def sensors_by_location(self):
        return self._sensors_by_location


    def sensors_by_integration(self):
        return self._sensors_by_integration
