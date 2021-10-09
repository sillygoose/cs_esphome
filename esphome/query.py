import logging
import datetime

from exceptions import InternalError


_LOGGER = logging.getLogger('cs_esp')


def create_point(measurement, tags, device, value, timestamp):
    lp_tags = ''
    separator = ''
    for tag in tags:
        lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
        separator = ','

    lp = f"{measurement}," + lp_tags + f" {device}={value} {timestamp}"
    return lp


def integrate_locations(query_api, bucket, locations, period):
    points = []
    for location, sensors in locations.items():
        if period == 'today':
            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f' |> filter(fn: (r) => r._location == "{location}")' \
            f' |> filter(fn: (r) => r._field == "today")' \
            f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        elif period == 'month':
            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f' |> filter(fn: (r) => r._location == "{location}")' \
            f' |> filter(fn: (r) => r._field == "month")' \
            f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        elif period == 'year':
            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f' |> filter(fn: (r) => r._location == "{location}")' \
            f' |> filter(fn: (r) => r._field == "year")' \
            f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        else:
            raise InternalError(f"Internal error detected, expected 'today'. 'month', or 'year'")

        tables = query_api.query(query)
        for table in tables:
            sum = 0.0
            for row in table.records:
                for sensor in sensors:
                    device = sensor.get('device', None)
                    value = row.values.get(device, None)
                    if value is None:
                        _LOGGER.debug(f"{period} {location}: sensor '{device}' not found")
                        continue
                    sum += value
                _LOGGER.debug(f"{period} {location}: {sum}")
                tags = [{'t': '_device', 'v': f'_{location}'}]
                point = create_point(measurement='energy', tags=tags, device=f'{period}', value=sum, timestamp=ts)
                points.append(point)
    return points


def integrate_devices(query_api, bucket, sensors, period):
    points = []
    for sensor in sensors:
        location = sensor.get('location')
        device = sensor.get('device')
        measurement = sensor.get('measurement')
        location_query = '' if len(location) == 0 else f' |> filter(fn: (r) => r._location == "{location}")'

        if period == 'today':
            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "{measurement}")' \
            f'{location_query}' \
            f' |> filter(fn: (r) => r._field == "{device}")' \
            f' |> integral(unit: 1h, column: "_value")'
        elif period == 'month':
            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f'{location_query}' \
            f' |> filter(fn: (r) => r._field == "today")' \
            f' |> filter(fn: (r) => r._device == "{device}")' \
            f' |> sum(column: "_value")'
        elif period == 'year':
            ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f' |> range(start: {ts})' \
            f' |> filter(fn: (r) => r._measurement == "energy")' \
            f'{location_query}' \
            f' |> filter(fn: (r) => r._field == "month")' \
            f' |> filter(fn: (r) => r._device == "{device}")' \
            f' |> sum(column: "_value")'
        else:
            raise InternalError(f"Internal error detected, expected 'today'. 'month', or 'year' for the period")

        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                value = row.values.get('_value')
                tags = [{'t': '_device', 'v': f'{device}'}]
                if len(location) > 0:
                     tags.append({'t': '_location', 'v': f'{location}'})
                point = create_point(measurement='energy', tags=tags, device=f'{period}', value=value, timestamp=ts)
                points.append(point)
    return points
