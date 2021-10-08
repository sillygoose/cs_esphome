import logging
import datetime

#from influxdb_client import WritePrecision, Point


_LOGGER = logging.getLogger('esphome')


def create_point(measurement, tags, device, value, timestamp):
    lp_tags = ''
    separator = ''
    for tag in tags:
        lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
        separator = ','

    lp = f"{measurement}," + lp_tags + f" {device}={value} {timestamp}"
    return lp


def integrate_location(query_api, bucket, sensors, period):
    if period == 'today':
        return integrate_location_today(query_api, bucket, sensors)
    elif period == 'month':
        return integrate_location_month(query_api, bucket, sensors)
    elif period == 'year':
        return integrate_location_year(query_api, bucket, sensors)
    return []


def integrate_location_today(query_api, bucket, locations):
    """Find the location daily integrations."""
    midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
    points = []
    for location, sensors in locations.items():
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {midnight})' \
        f' |> filter(fn: (r) => r._measurement == "energy")' \
        f' |> filter(fn: (r) => r._location == "{location}")' \
        f' |> filter(fn: (r) => r._field == "today")' \
        f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        tables = query_api.query(query)
        for table in tables:
            sum = 0.0
            for row in table.records:
                for sensor in sensors:
                    device = sensor.get('device', None)
                    value = row.values.get(device, None)
                    if value is None:
                        _LOGGER.debug(f"Today {location}: sensor '{device}' not found")
                        continue
                    sum += value
                _LOGGER.debug(f"Today {location}: {sum}")
                tags = [{'t': '_device', 'v': f'_{location}'}]
                point = create_point(measurement='energy', tags=tags, device='today', value=sum, timestamp=midnight)
                points.append(point)
    return points


def integrate_location_month(query_api, bucket, locations):
    """Find the location monthly integrations."""
    month_start = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
    points = []
    for location, sensors in locations.items():
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {month_start})' \
        f' |> filter(fn: (r) => r._measurement == "energy")' \
        f' |> filter(fn: (r) => r._location == "{location}")' \
        f' |> filter(fn: (r) => r._field == "month")' \
        f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        tables = query_api.query(query)
        for table in tables:
            sum = 0.0
            for row in table.records:
                for sensor in sensors:
                    device = sensor.get('device', None)
                    value = row.values.get(device, None)
                    if value is None:
                        _LOGGER.debug(f"Month {location}: sensor '{device}' not found")
                        continue
                    sum += value
                _LOGGER.debug(f"Month {location}: {sum}")
                tags = [{'t': '_device', 'v': f'_{location}'}]
                point = create_point(measurement='energy', tags=tags, device='month', value=sum, timestamp=month_start)
                points.append(point)
    return points


def integrate_location_year(query_api, bucket, locations):
    """Find the location yearly integrations."""
    year_start = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
    points = []
    for location, sensors in locations.items():
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {year_start})' \
        f' |> filter(fn: (r) => r._measurement == "energy")' \
        f' |> filter(fn: (r) => r._location == "{location}")' \
        f' |> filter(fn: (r) => r._field == "year")' \
        f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
        tables = query_api.query(query)
        for table in tables:
            sum = 0.0
            for row in table.records:
                for sensor in sensors:
                    device = sensor.get('device', None)
                    value = row.values.get(device, None)
                    if value is None:
                        _LOGGER.debug(f"Year {location}: sensor '{device}' not found")
                        continue
                    sum += value
                _LOGGER.debug(f"Year {location}: {sum}")
                tags = [{'t': '_device', 'v': f'_{location}'}]
                point = create_point(measurement='energy', tags=tags, device='year', value=sum, timestamp=year_start)
                points.append(point)
    return points


def integrate_sensor(query_api, bucket, sensors, period):
    if period == 'today':
        return integrate_sensor_today(query_api, bucket, sensors)
    elif period == 'month':
        return integrate_sensor_month(query_api, bucket, sensors)
    elif period == 'year':
        return integrate_sensor_year(query_api, bucket, sensors)
    return None


def integrate_sensor_today(query_api, bucket, sensors):
    """Find the sensor daily integrations."""
    midnight = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        location = sensor.get('location')
        device = sensor.get('device')
        measurement = sensor.get('measurement')
        location_query = '' if len(location) == 0 else f' |> filter(fn: (r) => r._location == "{location}")'
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {midnight})' \
        f' |> filter(fn: (r) => r._measurement == "{measurement}")' \
        f'{location_query}' \
        f' |> filter(fn: (r) => r._field == "{device}")' \
        f' |> integral(unit: 1h, column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Today {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_device', 'v': f'{device}'}]
                if len(location) > 0:
                     tags.append({'t': '_location', 'v': f'{location}'})
                point = create_point(measurement='energy', tags=tags, device='today', value=value, timestamp=midnight)
                points.append(point)
    return points


def integrate_sensor_month(query_api, bucket, sensors):
    """Find the sensor monthly integrations."""
    month_start = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        device = sensor.get('device')
        location = sensor.get('location')
        location_query = '' if len(location) == 0 else f' |> filter(fn: (r) => r._location == "{location}")'
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {month_start})' \
        f' |> filter(fn: (r) => r._measurement == "energy")' \
        f'{location_query}' \
        f' |> filter(fn: (r) => r._field == "today")' \
        f' |> filter(fn: (r) => r._device == "{device}")' \
        f' |> sum(column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Month {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_device', 'v': f'{device}'}]
                if len(location) > 0:
                     tags.append({'t': '_location', 'v': f'{location}'})
                points.append(create_point(measurement='energy', tags=tags, device='month', value=value, timestamp=month_start))
    return points


def integrate_sensor_year(query_api, bucket, sensors):
    """Find the sensor yearly integrations."""
    year_start = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
    points = []
    for sensor in sensors:
        device = sensor.get('device')
        location = sensor.get('location')
        location_query = '' if len(location) == 0 else f' |> filter(fn: (r) => r._location == "{location}")'
        query = f'from(bucket: "{bucket}")' \
        f' |> range(start: {year_start})' \
        f' |> filter(fn: (r) => r._measurement == "energy")' \
        f'{location_query}' \
        f' |> filter(fn: (r) => r._field == "month")' \
        f' |> filter(fn: (r) => r._device == "{device}")' \
        f' |> sum(column: "_value")'
        tables = query_api.query(query)
        for table in tables:
            for row in table.records:
                _LOGGER.debug(f"Year {device}: {row.values.get('_value'):.3f} Wh")
                value = row.values.get('_value')
                tags = [{'t': '_device', 'v': f'{device}'}]
                if len(location) > 0:
                     tags.append({'t': '_location', 'v': f'{location}'})
                points.append(create_point(measurement='energy', tags=tags, device='year', value=value, timestamp=year_start))
    return points
