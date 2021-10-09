# Interface CS/ESPHome to the InfluxDB database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import os
import time
import logging

from influxdb_client import InfluxDBClient, WritePrecision, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from readconfig import retrieve_options

from exceptions import FailedInitialization, InfluxDBWriteError, InfluxDBFormatError, InfluxDBInitializationError
from urllib3.exceptions import NewConnectionError


_LOGGER = logging.getLogger('cs_esphome')

_INFLUXDB2_OPTIONS = {
    'url': {'type': str, 'required': True},
    'token': {'type': str, 'required': True},
    'bucket': {'type': str, 'required': True},
    'org': {'type': str, 'required': True},
}

_DEBUG_ENV_VAR = 'CS_ESPHOME_DEBUG'
_DEBUG_OPTIONS = {
    'create_bucket': {'type': bool, 'required': False},
    'delete_bucket': {'type': bool, 'required': False},
}


class InfluxDB:
    def __init__(self, config):
        self._config = config
        self._client = None
        self._write_api = None
        self._query_api = None
        self._delete_api = None
        self._token = None
        self._org = None
        self._url = None
        self._bucket = None


    def start(self):
        """Initialize the InflixDB client."""
        try:
            influxdb_options = retrieve_options(self._config, 'influxdb2', _INFLUXDB2_OPTIONS)
            debug_options = retrieve_options(self._config, 'debug', _DEBUG_OPTIONS)
        except FailedInitialization as e:
            _LOGGER.error(f"{e}")
            return False

        result = False
        try:
            self._bucket = influxdb_options.get('bucket')
            self._url = influxdb_options.get('url')
            self._token = influxdb_options.get('token')
            self._org = influxdb_options.get('org')
            self._client = InfluxDBClient(url=self._url, token=self._token, org=self._org, enable_gzip=True)
            if not self._client:
                raise FailedInitialization(f"failed to get InfluxDBClient from '{self._url}' (check url, token, and/or organization)")
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            self._query_api = self._client.query_api()
            self._delete_api = self._client.delete_api()

            cs_esphome_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
            if cs_esphome_debug and debug_options.get('delete_bucket', None) and self.delete_bucket():
                _LOGGER.info(f"Deleted bucket '{self._bucket}' at '{self._url}'")

            if not self.connect_bucket(cs_esphome_debug and debug_options.get('create_bucket', None)):
                raise FailedInitialization(f"unable to access bucket '{self._bucket}' at '{self._url}'")
            _LOGGER.info(f"Connected to InfluxDB2: '{self._url}', bucket '{self._bucket}'")
            result = True

        except FailedInitialization as e:
            _LOGGER.error(f"InfluxDB2 client {e}")
            self._client = None
        except NewConnectionError:
            _LOGGER.error(f"InfluxDB2 client unable to connect to host at {self._url}")
        except ApiException as e:
            _LOGGER.error(f"InfluxDB2 client unable to access bucket '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception: {e}")
        finally:
            return result


    def stop(self):
        if self._write_api:
            self._write_api.close()
            self._write_api = None
        if self._client:
            self._client.close()
            self._client = None


    def bucket(self):
        return self._bucket


    def write_api(self):
        return self._write_api


    def query_api(self):
        return self._query_api


    def delete_api(self):
        return self._delete_api


    def write_point(self, measurement, tags, device, value, timestamp=None):
        """Write a single sensor to the database."""
        lp_tags = ''
        separator = ''
        for tag in tags:
            lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
            separator = ','
        lp = f"{measurement}," + lp_tags + f" {device}={value} {timestamp}"

        try:
            self._write_api.write(bucket=self._bucket, record=lp, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB2 client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_sensor(): {e}")


    def write_points(self, points):
        """Write a list of points to the database."""
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB2 client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_sensor(): {e}")


    def write_sensor(self, sensor, state, timestamp=None):
        """Write a sensor to the database."""
        ts = timestamp if timestamp is not None else int(time.time())

        measurement = sensor.get('measurement', None)
        device = sensor.get('device', None)
        location = sensor.get('location', None)
        precision = sensor.get('precision', None)
        if measurement is None or device is None:
            raise InfluxDBFormatError(f"'measurement' and/or 'device' are required")

        location_tag = '' if not location or not len(location) else f',_location={location}'
        value = round(state, precision) if ((precision != None) and isinstance(state, float)) else state
        lp = f'{measurement}{location_tag} {device}={value} {timestamp}'

        try:
            self._write_api.write(bucket=self._bucket, record=lp, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB2 client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_sensor(): {e}")


    def delete_bucket(self):
        buckets_api = self._client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(self._bucket)
        if bucket:
            buckets_api.delete_bucket(bucket)
            bucket = buckets_api.find_bucket_by_name(self._bucket)
            if not bucket:
                return True
        return False


    def connect_bucket(self, create_bucket=False):
        buckets_api = self._client.buckets_api()
        bucket = buckets_api.find_bucket_by_name(self._bucket)
        if bucket:
            return True
        if create_bucket:
            bucket = buckets_api.create_bucket(bucket_name=self._bucket, org_id=self._org, retention_rules=None, org=None)
            if bucket:
                _LOGGER.info(f"Created bucket '{self._bucket}' at {self._url}")
                return True
        return False