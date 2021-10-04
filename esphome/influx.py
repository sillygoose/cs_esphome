# Interface to InfluxDB esphome/cs24 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import os
import time
import datetime
import logging
from config import config_from_yaml

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from exceptions import FailedInitialization


_LOGGER = logging.getLogger('esphome')

LP_LOOKUP = {
    'ac_measurements/power': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'power', 'output': True},
    'ac_measurements/voltage': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'voltage', 'output': True},
    'ac_measurements/current': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'current', 'output': True},
    'ac_measurements/efficiency': {'measurement': 'ac_measurements', 'tags': ['_inverter'], 'field': 'efficiency', 'output': False},
    'dc_measurements/power': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'power', 'output': True},
    'dc_measurements/voltage': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'voltage', 'output': False},
    'dc_measurements/current': {'measurement': 'dc_measurements', 'tags': ['_inverter', '_string'], 'field': 'current', 'output': False},
}


class InfluxDB:
    def __init__(self):
        self._client = None
        self._write_api = None
        self._query_api = None
        self._enabled = False

    def __del__(self):
        if self._client:
            self._client.close()

    def check_config(self, influxdb2):
        """Check that the needed YAML options exist."""
        errors = False
        required = {'enable': bool, 'url': str, 'token': str, 'bucket': str, 'org': str}
        options = dict(influxdb2)
        for key in required:
            if key not in options.keys():
                _LOGGER.error(f"Missing required 'influxdb2' option in YAML file: '{key}'")
                errors = True
            else:
                v = options.get(key, None)
                if not isinstance(v, required.get(key)):
                    _LOGGER.error(f"Expected type '{required.get(key).__name__}' for option 'influxdb2.{key}'")
                    errors = True
        if errors:
            raise FailedInitialization(Exception("Errors detected in 'influxdb2' YAML options"))
        return options

    def start(self, config):
        self.check_config(config)
        if not config.enable:
            return True

        try:
            self._bucket = config.bucket
            self._client = InfluxDBClient(url=config.url, token=config.token, org=config.org)
            if not self._client:
                raise Exception(
                    f"Failed to get InfluxDBClient from {config.url} (check url, token, and/or organization)")

            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            if not self._write_api:
                raise Exception(f"Failed to get client write_api() object from {config.url}")

            query_api = self._client.query_api()
            if not query_api:
                raise Exception(f"Failed to get client query_api() object from {config.url}")
            try:
                query_api.query(f'from(bucket: "{self._bucket}") |> range(start: -1m)')
                _LOGGER.info(f"Connected to the InfluxDB database at {config.url}, bucket '{self._bucket}'")
            except Exception:
                raise Exception(f"Unable to access bucket '{self._bucket}' at {config.url}")

        except Exception as e:
            _LOGGER.error(f"{e}")
            self.stop()
            return False

        return True

    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            self._client.close()
            self._client = None

    def write_points(self, points):
        if not self._write_api:
            return False
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            result = True
        except Exception as e:
            _LOGGER.error(f"Database write() call failed in write_points(): {e}")
            result = False
        return result


if __name__ == "__main__":
    yaml_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'esphome.yaml')
    config = config_from_yaml(data=yaml_file, read_from_file=True)
    influxdb = InfluxDB()
    result = influxdb.start(config=config.esphome.influxdb2)
    if not result:
        print("Something failed during initialization")
    else:
        #influxdb.write_sma_sensors(testdata)
        influxdb.stop()
        print("Done")
