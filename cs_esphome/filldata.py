"""Utility function to fill in missing data points."""

import logging

import datetime
from dateutil.relativedelta import relativedelta

from influx import InfluxDB
from readconfig import read_config
import logfiles


_LOGGER = logging.getLogger('cs_esphome')


def fill_consumption_data(influxdb_client) -> None:
    """Fill in missing consumptiondata for Grafana."""
    parker_lane_monthly = [
        {'date': '2020-10-01', 'prod': 1531, 'billed': 41, 'cons': 1572},
        {'date': '2020-11-01', 'prod': 1131, 'billed': 900, 'cons': 2031},
        {'date': '2020-12-01', 'prod': 538, 'billed': 1867, 'cons': 2405},
        {'date': '2021-01-01', 'prod': 496, 'billed': 1948, 'cons': 2444},
        {'date': '2021-02-01', 'prod': 1348, 'billed': 887, 'cons': 2235},
        {'date': '2021-03-01', 'prod': 2577, 'billed': -635, 'cons': 1942},
        {'date': '2021-04-01', 'prod': 2621, 'billed': -650, 'cons': 1971},
        {'date': '2021-05-01', 'prod': 3260, 'billed': -1873, 'cons': 1387},
        {'date': '2021-06-01', 'prod': 3214, 'billed': -1551, 'cons': 1663},
        {'date': '2021-07-01', 'prod': 2786, 'billed': -1346, 'cons': 1440},
        {'date': '2021-08-01', 'prod': 2765, 'billed': -378, 'cons': 2387},
        {'date': '2021-09-01', 'prod': 2119, 'billed': -298, 'cons': 1821},
        {'date': '2021-10-01', 'prod': 1537, 'billed': 0, 'cons': 1603},
    ]
    parker_lane_daily = [
        {'date': '2021-10-22', 'prod': 1531, 'billed': 41, 'cons': 16},
        {'date': '2021-10-23', 'prod': 1531, 'billed': 41, 'cons': 52},
        {'date': '2021-10-24', 'prod': 1531, 'billed': 41, 'cons': 71},
        {'date': '2021-10-25', 'prod': 1531, 'billed': 41, 'cons': 38},
        {'date': '2021-10-26', 'prod': 1531, 'billed': 41, 'cons': 61},
        {'date': '2021-10-27', 'prod': 1531, 'billed': 41, 'cons': 70},
        {'date': '2021-10-28', 'prod': 1531, 'billed': 41, 'cons': 99},
        {'date': '2021-10-29', 'prod': 1531, 'billed': 41, 'cons': 84},
        {'date': '2021-10-30', 'prod': 1531, 'billed': 41, 'cons': 46},
        {'date': '2021-10-31', 'prod': 1531, 'billed': 41, 'cons': 37},
        {'date': '2021-11-01', 'prod': 1531, 'billed': 41, 'cons': 22},
    ]

    for day in parker_lane_daily:
        current = datetime.datetime.fromisoformat(day.get('date'))
        value = 1000.0 * day.get('cons')
        influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='today', value=value, timestamp=int(current.timestamp()))

    for month in parker_lane_monthly:
        current = datetime.datetime.fromisoformat(month.get('date'))
        value = 1000.0 * month.get('cons')
        influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='month', value=value, timestamp=int(current.timestamp()))


def fill_grafana_data(config, influxdb_client) -> None:
    """Fill in missing data for Grafana."""

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
            fill_consumption_data(influxdb_client)
            fill_grafana_data(config.cs_esphome, influxdb_client)
