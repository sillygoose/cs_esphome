import logging
import datetime

from influxdb_client import WritePrecision, Point


_LOGGER = logging.getLogger('esphome')


def create_point(measurement, tag, device, value, timestamp):
    tag_name = tag.get('t', '')
    tag_value = tag.get('v', '')
    if len(tag_name) and len(tag_value):
        point = Point(f"{measurement}").tag(f"{tag_name}", f"{tag_value}").field(f"{device}", value).time(timestamp, write_precision=WritePrecision.S)
    else:
        point = Point(f"{measurement}").field(f"{device}", value).time(timestamp, write_precision=WritePrecision.S)
    return point


def integrate(query_api, bucket, sensors):
    midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())

    points = []
    for sensor in sensors:
        location = sensor.get('location')
        device = sensor.get('device')
        measurement = sensor.get('measurement')
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {midnight})' \
        f' |> filter(fn: (r) => r["_measurement"] == "{measurement}")' \
        f' |> filter(fn: (r) => r["_field"] == "{device}")' \
        f' |> filter(fn: (r) => r["_location"] == "{location}")' \
        f' |> integral(unit: 1h, column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.info(f"{device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                point = create_point(measurement=measurement, tag={'t': '_integral', 'v': 'today'}, device=device, value=value, timestamp=midnight)
                points.append(point)

    return points