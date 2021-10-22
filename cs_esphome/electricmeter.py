"""Electric Meter class."""

import logging
import datetime


_LOGGER = logging.getLogger('cs_esphome')


class ElectricMeter():
    """Class to model the electric meter."""

    def __init__(self, config, influxdb_client):
        """Create a new ESPHomeApi object."""
        self._config = config
        self._influxdb_client = influxdb_client


    def start(self):
        """."""
        config = self._config
        if 'meter' in config.keys():
            if 'enable_setting' in config.meter.keys():
                enable_setting = config.meter.get('enable_setting', None)
                if enable_setting:
                    if 'value' in config.meter.keys():
                        reading = config.meter.get('value', None)
                        midnight = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0))
                        self._influxdb_client.write_point(measurement='energy', tags=[{'t': '_meter', 'v': 'reading'}], field='today', value=reading, timestamp=int(midnight.timestamp()))
                        _LOGGER.info(f"Meter set to {reading} kWh")
                    else:
                        _LOGGER.warning("Expected meter value in YAML file, no action taken")
        return True