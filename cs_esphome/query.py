import logging
import datetime

from exceptions import InternalError
from influxdb_client.rest import ApiException


_LOGGER = logging.getLogger('cs_esphome')


def create_point(measurement, tags, device, value, timestamp):
    lp_tags = ''
    separator = ''
    for tag in tags:
        lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
        separator = ','

    lp = f"{measurement}," + lp_tags + f" {device}={value} {timestamp}"
    return lp


def execute_query(query_api, query):
    tables = []
    try:
        tables = query_api.query(query)
    except ApiException as e:
        _LOGGER.error(f"InfluxDB2 query error: {e.reason}")
    except Exception as e:
        raise InternalError(f"{e}")
    return tables
