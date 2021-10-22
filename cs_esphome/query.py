import logging
import datetime
import asyncio
import time

from influxdb_client.rest import ApiException

from exceptions import InternalError, InfluxDBQueryError, InfluxDBWriteError


_LOGGER = logging.getLogger('cs_esphome')


class QueryManager():
    """Class to create and manage InfluxDB queries."""

    _DEFAULT_SAMPLING_QUERIES = 60

    def __init__(self, config, influxdb_client):
        """Create a new QueryManager object."""
        self._config = config
        self._influxdb_client = influxdb_client
        self._sampling_queries = QueryManager._DEFAULT_SAMPLING_QUERIES


    async def start(self, locations):
        """Create a new QueryManager object."""
        self._query_api = self._influxdb_client.query_api()
        self._bucket = self._influxdb_client.bucket()
        self._locations = locations

        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_queries = config.settings.sampling.get('queries', QueryManager._DEFAULT_SAMPLING_QUERIES)

        return True


    async def run(self):
        try:
            _LOGGER.info(f"CS/ESPHome Query Manager starting up, query tasks will run every {self._sampling_queries} seconds")
            await self.scheduler()
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run(): {e}")


    async def scheduler(self):
        """Task to schedule actions at regular intervals."""
        SLEEP = 0.5
        last_tick = time.time_ns() // 1000000000
        while True:
            tick = time.time_ns() // 1000000000
            if tick != last_tick:
                last_tick = tick
                if tick % self._sampling_queries == 0:
                    self.process_locations_power()
                    self.process_locations_energy(period='today')
                    self.process_locations_energy(period='month')
                    self.process_locations_energy(period='year')
            await asyncio.sleep(SLEEP)


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


    # _measurement="power", _location={location}, _field="now"  (sum of device power field)
    def process_locations_power(self)-> None:
        bucket = self._bucket
        locations = self._locations
        points = []
        for location, sensors in locations.items():
            ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
            query = f'from(bucket: "{bucket}")' \
            f'  |> range(start: -1m)' \
            f'  |> filter(fn: (r) => r._measurement == "power" and r._location == "{location}" and r._field == "sample")' \
            f'  |> last()' \
            f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")'

            try:
                tables = self._query_api.query(query)
                for table in tables:
                    sum = 0.0
                    for row in table.records:
                        for sensor in sensors:
                            device = sensor.get('device', None)
                            value = row.values.get(device, None)
                            if value is None:
                                # _LOGGER.debug(f"process_locations_power {location}: sensor '{device}' not found")
                                continue
                            sum += value
                        # _LOGGER.debug(f"Total power right now in '{location}': {sum:.0f}")
                        tags = [{'t': '_location', 'v': f'{location}'}]
                        point = self._create_point(measurement='power', tags=tags, field=f'now', value=sum, timestamp=ts)
                        points.append(point)

                try:
                    self._influxdb_client.write_points(points=points)
                except InfluxDBWriteError as e:
                    _LOGGER.error(f"InfluxDB client unable to write to '{self._bucket}': {e.reason}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception writing points in process_locations_power(): {e}")
            except ApiException as e:
                _LOGGER.error(f"Processing query error in process_locations_power() for location '{location}': {e.reason}")
                _LOGGER.error(f"query={query}'")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception processing query in process_locations_power(): {e}")


    # _measurement="energy", _location={location}, _field="today"  (sum of device energy fields)
    def process_locations_energy(self, period):
        bucket = self._bucket
        locations = self._locations
        points = []
        for location, sensors in locations.items():
            if period == 'today':
                ts = int(datetime.datetime.combine(datetime.datetime.now(), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f'  |> range(start: {ts})' \
                f'  |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and exists r._device and r._field == "today")' \
                f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'month':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f'  |> range(start: {ts})' \
                f'  |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and exists r._device and r._field == "month")' \
                f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")'
            elif period == 'year':
                ts = int(datetime.datetime.combine(datetime.datetime.now().replace(month=1, day=1), datetime.time(0, 0)).timestamp())
                query = f'from(bucket: "{bucket}")' \
                f'  |> range(start: {ts})' \
                f'  |> filter(fn: (r) => r._measurement == "energy" and r._location == "{location}" and exists r._device and r._field == "year")' \
                f'  |> pivot(rowKey:["_time"], columnKey: ["_device"], valueColumn: "_value")'
            else:
                raise InternalError(f"expected 'today'. 'month', or 'year'")

            try:
                tables = self._query_api.query(query)
                for table in tables:
                    sum = 0.0
                    for row in table.records:
                        for sensor in sensors:
                            device = sensor.get('device', None)
                            value = row.values.get(device, None)
                            if value is None:
                                # _LOGGER.debug(f"process_locations_energy() {period} {location}: sensor '{device}' not found")
                                continue
                            sum += value
                        # _LOGGER.debug(f"Total energy {period} in '{location}': {sum}")
                        tags = [{'t': '_location', 'v': f'{location}'}]
                        point = self._create_point(measurement='energy', tags=tags, field=f'{period}', value=sum, timestamp=ts)
                        points.append(point)
                try:
                    self._influxdb_client.write_points(points=points)
                except InfluxDBWriteError as e:
                    _LOGGER.error(f"InfluxDB client unable to write to '{self._bucket}': {e.reason}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception writing points in process_locations_energy(): {e}")
            except ApiException as e:
                _LOGGER.error(f"Processing query error in process_locations_energy() for location '{location}': {e.reason}")
                _LOGGER.error(f"query={query}'")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception processing query in process_locations_energy(): {e}")

