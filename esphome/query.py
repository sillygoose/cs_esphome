import logging
import datetime

from influxdb_client import WritePrecision, Point


_LOGGER = logging.getLogger('esphome')


def create_point(measurement, tags, device, value, timestamp):
    lp_tags = ''
    separator = ''
    for tag in tags:
        lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
        separator = ','

    lp = f"{measurement}," + lp_tags + f" {device}={value} {timestamp}"
    return lp


def integrate_today(query_api, bucket, sensors):
    """Find the sensor daily integrations."""
    midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        location = sensor.get('location')
        device = sensor.get('device')
        measurement = sensor.get('measurement')
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {midnight})' \
        f' |> filter(fn: (r) => r._measurement == "{measurement}")' \
        f' |> filter(fn: (r) => r._location == "{location}")' \
        f' |> filter(fn: (r) => r._integral != "today" and r._integral != "month" and r._integral != "year")' \
        f' |> filter(fn: (r) => r._field == "{device}")' \
        f' |> integral(unit: 1h, column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Today {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_integral', 'v': 'today'}, {'t': '_location', 'v': f'{location}'}]
                point = create_point(measurement=measurement, tags=tags, device=device, value=value, timestamp=midnight)
                points.append(point)
    return points


def integrate_month(query_api, bucket, sensors):
    """Find the sensor monthly integrations."""
    month_start = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        device = sensor.get('device')
        location = sensor.get('location')
        measurement = sensor.get('measurement')
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {month_start})' \
        f' |> filter(fn: (r) => r["_measurement"] == "{measurement}")' \
        f' |> filter(fn: (r) => r["_field"] == "{device}")' \
        f' |> filter(fn: (r) => r["_integral"] == "today")' \
        f' |> sum(column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Month {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_integral', 'v': 'month'}, {'t': '_location', 'v': f'{location}'}]
                points.append(create_point(measurement=measurement, tags=tags, device=device, value=value, timestamp=month_start))
    return points


def integrate_year(query_api, bucket, sensors):
    """Find the sensor monthly integrations."""
    year_start = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        device = sensor.get('device')
        location = sensor.get('location')
        measurement = sensor.get('measurement')
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {year_start})' \
        f' |> filter(fn: (r) => r["_measurement"] == "{measurement}")' \
        f' |> filter(fn: (r) => r["_field"] == "{device}")' \
        f' |> filter(fn: (r) => r["_integral"] == "month")' \
        f' |> sum(column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Year {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_integral', 'v': 'year'}, {'t': '_location', 'v': f'{location}'}]
                points.append(create_point(measurement=measurement, tags=tags, device=device, value=value, timestamp=year_start))
    return points
