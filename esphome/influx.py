# Interface to InfluxDB esphome/cs24 database
#
# InfluxDB Line Protocol Reference
# https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/

from os import name
import time
import logging

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.rest import ApiException

from exceptions import FailedInitialization, InfluxDBWriteError
from urllib3.exceptions import NewConnectionError


_LOGGER = logging.getLogger('esphome')

LP_LOOKUP = {
    'cs24/power': {'measurement': 'power', 'field': 'power', 'output': True},
    'cs24/voltage': {'measurement': 'voltage', 'field': 'voltage', 'output': True},
}


def check_config(influxdb2) -> bool:
    """Check that the needed YAML options exist."""
    errors = False
    required = {'url': str, 'token': str, 'bucket': str, 'org': str}
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
        raise FailedInitialization(f"one or more errors detected in 'influxdb2' YAML options")
    return options


def delete_bucket(api, name):
    bucket = api.find_bucket_by_name(name)
    if bucket:
        api.delete_bucket(bucket)
        bucket = api.find_bucket_by_name(name)
        if not bucket:
            return True
    return False


def create_bucket():
    return


class InfluxDB:
    def __init__(self):
        self._client = None
        self._write_api = None
        self._enabled = False
        self._token = None
        self._org = None
        self._url = None
        self._bucket = None

    def __del__(self):
        if self._client:
            self._client.close()

    def start(self, config):
        """Initilaze the InflixDB client."""
        check_config(config)

        result = False
        try:
            self._bucket = config.bucket
            self._url = config.url
            self._token = config.token
            self._org = config.org
            self._client = InfluxDBClient(url=self._url, token=self._token, org=self._org)
            if not self._client:
                raise FailedInitialization(f"failed to get InfluxDBClient from {self._url} (check url, token, and/or organization)")

            if config.get('new_bucket', None) and delete_bucket(api=self._client.buckets_api(), name=self._bucket):
                _LOGGER.info(f"Deleted bucket '{self._bucket}' at {self._url}")

            self._write_api = self._client.write_api(write_options=SYNCHRONOUS)

            for i in range(1, 3):
                try:
                    query_api = self._client.query_api()
                    query_api.query(f'from(bucket: "{self._bucket}") |> range(start: -1m)')
                    _LOGGER.info(f"Connected to InfluxDB2: {self._url}, bucket '{self._bucket}'")
                    break
                except ApiException as e:
                    buckets_api = self._client.buckets_api()
                    bucket = buckets_api.create_bucket(bucket_name=self._bucket, org_id=self._org, retention_rules=None, org=None)
                    if bucket:
                        _LOGGER.info(f"Created missing bucket '{self._bucket}' at {self._url}")
                    continue
            else:
                FailedInitialization(f"unable to access bucket '{self._bucket}' at {self._url}: {e.reason}")

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

    def write_sensor(self, sensor, state, timestamp=None):
        if not self._write_api:
            return False
        ts = timestamp if timestamp is not None else int(time.time())

        measurement = sensor.get('measurement', None)
        device = sensor.get('device', None)
        location = sensor.get('location', None)
        precision = sensor.get('precision', None)
        v = round(state, precision) if isinstance(state, float) else state

        if measurement is None or device is None or location is None or precision is None:
            return False

        lp = f"{measurement}"
        if location and len(location):
            lp += f",_location={location}"

        if isinstance(v, int):
            lp += f" {device}={v}i {ts}"
        elif isinstance(v, float):
            lp += f" {device}={v} {ts}"
        else:
            _LOGGER.error(f"write_sensor(): unanticipated type '{type(v)}' in measurement '{measurement}/{device}'")
            return False

        points = []
        points.append(lp)
        try:
            self._write_api.write(bucket=self._bucket, record=points, write_precision=WritePrecision.S)
            return True
        except ApiException as e:
            raise InfluxDBWriteError(f"InfluxDB2 client unable to write to '{self._bucket}' at {self._url}: {e.reason}")
        except Exception as e:
            _LOGGER.error(f"Database write() call failed in write_sensor(): {e}")
            return False
