# Interface CS/ESPHome to the InfluxDB database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

import os
import time
import logging

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from readconfig import retrieve_options
from readconfig import read_config
import logfiles

from exceptions import FailedInitialization
from exceptions import InfluxDBWriteError, InfluxDBFormatError, InfluxDBBucketError

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
        self._tasks_api = None
        self._organizations_api = None
        self._token = None
        self._org = None
        self._url = None
        self._bucket = None

    def start(self):
        """Initialize the InfluxDB client."""
        try:
            influxdb_options = retrieve_options(self._config, 'influxdb2', _INFLUXDB2_OPTIONS)
            debug_options = retrieve_options(self._config, 'debug', _DEBUG_OPTIONS)
        except FailedInitialization as e:
            _LOGGER.error(f"{e}")
            return False

        if len(influxdb_options.keys()) == 0:
            raise FailedInitialization("missing 'influxdb2' options")

        result = False
        try:
            self._bucket = influxdb_options.get('bucket', None)
            self._url = influxdb_options.get('url', None)
            self._token = influxdb_options.get('token', None)
            self._org = influxdb_options.get('org', None)
            self._client = InfluxDBClient(url=self._url, token=self._token, org=self._org, enable_gzip=True)
            if not self._client:
                raise FailedInitialization(f"failed to get InfluxDBClient from '{self._url}' (check url, token, and/or organization)")
            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
            self._query_api = self._client.query_api()
            self._delete_api = self._client.delete_api()
            self._tasks_api = self._client.tasks_api()
            self._organizations_api = self._client.organizations_api()

            cs_esphome_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
            try:
                if cs_esphome_debug and debug_options.get('delete_bucket', False):
                    self.delete_bucket()
                    _LOGGER.info(f"Deleted bucket '{self._bucket}' at '{self._url}'")
            except InfluxDBBucketError as e:
                raise FailedInitialization(f"{e}")

            try:
                if not self.connect_bucket(cs_esphome_debug and debug_options.get('create_bucket', False)):
                    raise FailedInitialization(f"Unable to access (or create) bucket '{self._bucket}' at '{self._url}'")
            except InfluxDBBucketError as e:
                raise FailedInitialization(f"{e}")

            try:
                organizations = self._organizations_api.find_organizations(org=self._org)
                if not organizations:
                    raise FailedInitialization(f"Unable to access organizations at '{self._url}' (check token permissions)")
            except InfluxDBBucketError as e:
                raise FailedInitialization(f"{e}")

            _LOGGER.info(f"Connected to InfluxDB: '{self._url}', bucket '{self._bucket}'")
            result = True

        except FailedInitialization as e:
            _LOGGER.error(f" client {e}")
            self._client = None
        except NewConnectionError:
            _LOGGER.error(f"InfluxDB client unable to connect to host at {self._url}")
        except ApiException as e:
            _LOGGER.error(f"InfluxDB client unable to access bucket '{self._bucket}' at {self._url}: {e.reason}")
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

    def org(self):
        return self._org

    def write_api(self):
        return self._write_api

    def query_api(self):
        return self._query_api

    def delete_api(self):
        return self._delete_api

    def tasks_api(self):
        return self._tasks_api

    def organizations_api(self):
        return self._organizations_api

    def write_point(self, measurement, tags, field, value, timestamp=None):
        """Write a single sensor to the database."""
        timestamp = timestamp if timestamp is not None else int(time.time())
        lp_tags = ''
        separator = ''
        for tag in tags:
            lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
            separator = ','
        lp = f"{measurement}," + lp_tags + f" {field}={value} {timestamp}"

        try:
            self._write_api.write(bucket=self._bucket, record=lp, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_point(): {e}")

    def write_points(self, points):
        """Write a list of points to the database."""
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_points(): {e}")

    def write_batch_sensors(self, batch_sensors, timestamp=None):
        """Write a batch of sensors to the database."""

        if len(batch_sensors) == 0:
            return

        timestamp = timestamp if timestamp is not None else int(time.time())

        batch = []
        for record in batch_sensors:
            sensor = record.get('sensor', None)
            state = record.get('state', None)
            measurement = sensor.get('measurement', None)
            device = sensor.get('device', None)
            location = sensor.get('location', None)
            precision = sensor.get('precision', None)
            if measurement is None or device is None:
                raise InfluxDBFormatError("'measurement' and/or 'device' are required")

            location_tag = '' if not location or not len(location) else f',_location={location}'
            device_tag = f',_device={device}'
            value = round(state, precision) if ((precision is not None) and isinstance(state, float)) else state
            lp = f'{measurement}{device_tag}{location_tag} sample={value} {timestamp}'
            batch.append(lp)

        try:
            self._write_api.write(bucket=self._bucket, record=batch, write_precision=WritePrecision.S)
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBWriteError(f"Unexpected failure in write_batch_sensors(): {e}")

    def delete_bucket(self):
        try:
            buckets_api = self._client.buckets_api()
            found_bucket = buckets_api.find_bucket_by_name(self._bucket)
            if found_bucket:
                buckets_api.delete_bucket(found_bucket)
                bucket = buckets_api.find_bucket_by_name(self._bucket)
                if not bucket:
                    return True
            return False
        except ApiException as e:
            raise InfluxDBBucketError(f"InfluxDB client unable to delete bucket '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBBucketError(f"Unexpected exception in delete_bucket(): {e}")

    def connect_bucket(self, create_bucket=False):
        try:
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
        except ApiException as e:
            raise InfluxDBBucketError(f"InfluxDB client unable to create bucket '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            raise InfluxDBBucketError(f"Unexpected exception in connect_bucket(): {e}")


if __name__ == "__main__":
    logfiles.start()
    config = read_config()
    if config:
        if 'cs_esphome' in config.keys() and 'influxdb2' in config.cs_esphome.keys():
            influxdb_client = InfluxDB(config.cs_esphome)
            influxdb_client.start()
