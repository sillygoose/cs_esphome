import logging
import datetime

from exceptions import InternalError, InfluxDBQueryError
from influxdb_client.rest import ApiException


_LOGGER = logging.getLogger('cs_esphome')


class QueryManager():
    """Class to create and manage InfluxDB queries."""


    def __init__(self, config, influxdb_client):
        """Create a new QueryManager object."""
        self._config = config
        self._influxdb_client = influxdb_client


    async def start(self, locations):
        """Create a new QueryManager object."""
        self._query_api = self._influxdb_client.query_api()
        self._bucket = self._influxdb_client.bucket()
        self._locations = locations

        #self.process_locations_power(period='today')
        #self.process_locations_energy(period='today')
        return True


    async def run(self):
        try:
            await self.influx_tasks()
        except FailedInitialization as e:
            _LOGGER.error(f"run(): {e}")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")


    def _create_point(self, measurement, tags, field, value, timestamp):
        lp_tags = ''
        separator = ''
        for tag in tags:
            lp_tags += f"{separator}{tag.get('t')}={tag.get('v')}"
            separator = ','

        lp = f"{measurement}," + lp_tags + f" {field}={value} {timestamp}"
        return lp


    def execute_query(self, query_api, query):
        tables = []
        try:
            tables = query_api.query(query)
        except ApiException as e:
            raise InfluxDBQueryError(f"InfluxDB query error: {e.reason}")
        except Exception as e:
            raise InternalError(f"{e}")
        return tables


    # _measurement="power", _location={location}, _field="today"  (sum of device power field)
    def process_locations_power(self, period):
        bucket = self._bucket
        locations = self._locations
        for location, sensors in locations.items():
            if period == 'today':
                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "power" and r._location == "{location}" and r._field == "sample")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'month':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "power" and r._location == "{location}" and r._field == "today")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'year':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "power" and r._location == "{location}" and r._field == "month")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            else:
                raise InternalError(f"expected 'today'. 'month', or 'year'")

            try:
                query_api = self._query_api
                tables = query_api.query(query)
                points = []
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
                        _LOGGER.debug(f"Total power {period} in '{location}': {sum}")
                        tags = [{'t': '_location', 'v': f'{location}'}]
                        point = self._create_point(measurement='power', tags=tags, field=f'{period}', value=sum, timestamp=ts)
                        points.append(point)
                return points
            except ApiException as e:
                raise InfluxDBQueryError(f"InfluxDB client reported an error in process_locations_power(): {e.reason}")
            except Exception as e:
                raise InfluxDBQueryError(f"Unexpected exception in process_locations_power(): {e}")


    # _measurement="energy", _location={location}, _field="today"  (sum of device energy fields)
    def process_locations_energy(self, period):
        bucket = self._bucket
        locations = self._locations
        for location, sensors in locations.items():
            if period == 'today':
                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and r._field == "today")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'month':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and r._field == "month")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'year':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f' |> range(start: {ts})' \
                f' |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and r._field == "year")' \
                f' |> pivot(rowKey:["_field"], columnKey: ["_device"], valueColumn: "_value")'
            else:
                raise InternalError(f"expected 'today'. 'month', or 'year'")

            try:
                query_api = self._query_api
                tables = query_api.query(query)
                points = []
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
                        _LOGGER.debug(f"Total energy {period} in '{location}': {sum}")
                        tags = [{'t': '_location', 'v': f'{location}'}]
                        point = self._create_point(measurement='energy', tags=tags, field=f'{period}', value=sum, timestamp=ts)
                        points.append(point)
                return points
            except ApiException as e:
                raise InfluxDBQueryError(f"InfluxDB client reported an error in process_locations_power(): {e.reason}")
            except Exception as e:
                raise InfluxDBQueryError(f"Unexpected exception in process_locations_power(): {e}")
