"""Utility function to fill in missing data points."""


import os
import logging

import datetime
from dateutil.relativedelta import relativedelta

from influx import InfluxDB

from readconfig import retrieve_options
from readconfig import read_config
import logfiles


_LOGGER = logging.getLogger('cs_esphome')


def filldata(config, influxdb_client) -> None:
    """Fill in missing data for Grafana."""

    _DEBUG_ENV_VAR = 'CS_ESPHOME_DEBUG'
    _DEBUG_OPTIONS = {
        'fill_data': {'type': bool, 'required': False},
    }
    #debug_options = retrieve_options(config, 'debug', _DEBUG_OPTIONS)
    #cs_esphome_debug = os.getenv(_DEBUG_ENV_VAR, 'False').lower() in ('true', '1', 't')
    #if cs_esphome_debug is False or debug_options.get('fill_data', False) is False:
    #    return

    start = datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)) - relativedelta(months=13)
    stop = datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0))

    query_api = influxdb_client.query_api()
    bucket = influxdb_client.bucket()
    check_query = f'from(bucket: "{bucket}")' \
        f' |> range(start: 0)' \
        f' |> filter(fn: (r) => r._measurement == "energy" and r._device == "line" and r._field == "today")' \
        f' |> first()'
    tables = []
    try:
        tables = query_api.query(check_query)
    except Exception as e:
        raise Exception(f"Unexpected exception in filldata(): {e}")

    for table in tables:
        for row in table.records:
            utc = row.values.get('_time')
            stop = datetime.datetime(year=utc.year, month=utc.month, day=utc.day)

    if stop > start:
        _LOGGER.info(f"CS/ESPHome missing data fill: {start.date()} to {stop.date()}")
        current = start.replace(month=1, day=1)
        while current < stop:
            influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='year', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(years=1)

        current = start.replace(day=1)
        while current < stop:
            influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='month', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(months=1)

        current = start
        while current < stop:
            influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='today', value=0.0, timestamp=int(current.timestamp()))
            current += relativedelta(days=1)


if __name__ == "__main__":
    logfiles.start()
    config = read_config()
    if config:
        if 'cs_esphome' in config.keys() and 'influxdb2' in config.cs_esphome.keys():
            influxdb_client = InfluxDB(config.cs_esphome)
            influxdb_client.start()
            filldata(config.cs_esphome, influxdb_client)
