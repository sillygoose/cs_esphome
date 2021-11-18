"""Utility function to fill in missing data points."""

import logging

import datetime
from dateutil.relativedelta import relativedelta

from influx import InfluxDB
from readconfig import read_config
import logfiles


_LOGGER = logging.getLogger('cs_esphome')


def fill_consumption_data(influxdb_client) -> None:
    """Fill in known and back consumption data for Grafana."""
    parker_lane_monthly = [
        {'date': '2020-10-01', 'cons': 1572},
        {'date': '2020-11-01', 'cons': 2031},
        {'date': '2020-12-01', 'cons': 2405},
        {'date': '2021-01-01', 'cons': 2444},
        {'date': '2021-02-01', 'cons': 2235},
        {'date': '2021-03-01', 'cons': 1942},
        {'date': '2021-04-01', 'cons': 1971},
        {'date': '2021-05-01', 'cons': 1387},
        {'date': '2021-06-01', 'cons': 1663},
        {'date': '2021-07-01', 'cons': 1440},
        {'date': '2021-08-01', 'cons': 2387},
        {'date': '2021-09-01', 'cons': 1821},
        {'date': '2021-10-01', 'cons': 1603},
    ]
    parker_lane_daily = [
        {'date': '2021-10-22', 'cons': 16},
        {'date': '2021-10-23', 'cons': 52},
        {'date': '2021-10-24', 'cons': 71},
        {'date': '2021-10-25', 'cons': 38},
        {'date': '2021-10-26', 'cons': 61},
        {'date': '2021-10-27', 'cons': 70},
        {'date': '2021-10-28', 'cons': 99},
        {'date': '2021-10-29', 'cons': 84},
        {'date': '2021-10-30', 'cons': 46},
        {'date': '2021-10-31', 'cons': 37},
        {'date': '2021-11-01', 'cons': 87},
        {'date': '2021-11-02', 'cons': 45},
        {'date': '2021-11-03', 'cons': 103},
        {'date': '2021-11-04', 'cons': 92},
        {'date': '2021-11-05', 'cons': 44},
        {'date': '2021-11-06', 'cons': 49},
        {'date': '2021-11-07', 'cons': 78},
        {'date': '2021-11-08', 'cons': 43},
        {'date': '2021-11-09', 'cons': 39},
        {'date': '2021-11-10', 'cons': 85},
        {'date': '2021-11-11', 'cons': 109},
        {'date': '2021-11-12', 'cons': 23},
        {'date': '2021-11-13', 'cons': 48},
        {'date': '2021-11-14', 'cons': 50},
        {'date': '2021-11-15', 'cons': 88},
        {'date': '2021-11-16', 'cons': 56},
        {'date': '2021-11-17', 'cons': 72},
    ]

    for day in parker_lane_daily:
        current = datetime.datetime.fromisoformat(day.get('date'))
        value = 1000.0 * day.get('cons')
        influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='today', value=value, timestamp=int(current.timestamp()))
    _LOGGER.info("Past daily consumption written")

    for month in parker_lane_monthly:
        current = datetime.datetime.fromisoformat(month.get('date'))
        value = 1000.0 * month.get('cons')
        influxdb_client.write_point(measurement='energy', tags=[{'t': '_device', 'v': 'line'}], field='month', value=value, timestamp=int(current.timestamp()))
    _LOGGER.info("Past monthly consumption written")


def fill_grafana_data(influxdb_client) -> None:
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
            fill_grafana_data(influxdb_client)
