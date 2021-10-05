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
    'cs24/power': {'measurement': 'power', 'field': 'power', 'output': True},
    'cs24/voltage': {'measurement': 'voltage', 'field': 'voltage', 'output': True},
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

    def write_sensor(self, sensor, state, timestamp=None):
        if not self._write_api:
            return False

        ts = timestamp if timestamp is not None else int(time.time())

        measurement = sensor.get('measurement', None)
        tag = sensor.get('tag', None)
        field = sensor.get('field', None)
        precision = sensor.get('precision', None)
        v = round(state, precision) if isinstance(state, float) else state

        if measurement is None or tag is None or field is None or precision is None:
            return False

        lp = f"{measurement}"
        if tag and len(tag):
            lp += f",_location={tag}"

        if isinstance(v, int):
            lp += f" {field}={v}i {ts}"
        elif isinstance(v, float):
            lp += f" {field}={v} {ts}"
        else:
            _LOGGER.error(f"write_sensor(): unanticipated type '{type(v)}' in measurement '{measurement}/{field}'")
            return False

        points = []
        points.append(lp)
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            return True
        except Exception as e:
            _LOGGER.error(f"Database write() call failed in write_sensor(): {e}")
            return False
