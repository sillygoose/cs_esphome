"""Electric Meter class."""

import logging
import datetime
import json
import asyncio

from influxdb_client.rest import ApiException


_LOGGER = logging.getLogger('cs_esphome')


class UtilityMeter():
    """Class to model the utility meter."""

    _DEFAULT_SAMPLING_DELTA_WH = 150

    def __init__(self, config, base_name):
        """Create a new UtilityMeter object."""
        self._config = config
        self._base_name = base_name
        self._sampling_delta_wh = UtilityMeter._DEFAULT_SAMPLING_DELTA_WH
        self._task_gather = None

    def start(self, tasks_api, organization, bucket):
        """."""
        self._tasks_api = tasks_api
        self._organization = organization
        self._bucket = bucket

        config = self._config
        if 'settings' in config.keys():
            if 'sampling' in config.settings.keys():
                self._sampling_delta_wh = config.settings.sampling.get('delta_wh', UtilityMeter._DEFAULT_SAMPLING_DELTA_WH)

        _LOGGER.info(f"CS/ESPHome Utility Meter starting up, utility meter update task will run every {self._sampling_delta_wh} seconds")
        return True

    async def run_tasks(self):
        """Run the utility meter tasks."""
        try:
            self._task_gather = asyncio.gather(
                self.influx_meter_writing(),
                self.influx_meter_reading(),
                self.influx_delta_wh_tasks(),
            )
            await self._task_gather
            _LOGGER.debug("Utility meter tasks updated")
        except Exception as e:
            _LOGGER.error(f"Unexpected exception in run_tasks(): {e}")

    def stop(self):
        """Shutdown the UtilityMeter."""
        if self._task_gather:
            self._task_gather.cancel()
            self._task_gather = None

    async def influx_meter_writing(self):
        """Creates a task that updates the meter reading for the day (should be the value at midnight)."""
        tasks_api = self._tasks_api
        organization = self._organization
        bucket = self._bucket

        measurement = 'energy'
        tag_key = '_meter'
        output_value = 'writing'
        period = 'today'

        task_name = self._base_name + '.' + tag_key + '.' + output_value + '.' + measurement + '.' + period
        tasks = tasks_api.find_tasks(name=task_name)
        if tasks is None or len(tasks) == 0:
            _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
            flux = \
                '\n' \
                '// Supply the new meter reading (in kWh) for midnight today and run the task manually.' \
                '//' \
                '// If running this task at midnight you have no adjustments and can just enter the new reading but' \
                '// later in the day you must factor in the change due to production and consumption since midnight. ' \
                '// For example, if the current meter reading is 100 and you have consumed 10 kWh of energy today, ' \
                '// set the new_reading value to 90, this is the reading the meter would have at midnight.' \
                '\n' \
                'new_reading = 0\n' \
                '\n' \
                f'from(bucket: "{bucket}")\n' \
                f'  |> range(start: -1d)\n' \
                f'  |> filter(fn: (r) => r._measurement == "energy" and r._meter == "reading" and r._field == "today")\n' \
                f'  |> map(fn: (r) => ({{ _time: r._time, _field: r._field, _meter: r._meter, _measurement: r._measurement, _value: float(v: new_reading) * 1000.0 }}))\n' \
                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'

            try:
                tasks_api.create_task_every(name=task_name, flux=flux, every='1y', organization=organization)
                # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"ApiException during task creation in _delta_wh_worker(): {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation in _delta_wh_worker(): {e}")

    async def influx_meter_reading(self):
        """Creates the cron task that updates the meter reading at midnight."""
        tasks_api = self._tasks_api
        organization = self._organization
        bucket = self._bucket

        measurement = 'energy'
        tag_key = '_meter'
        output_value = 'reading'
        period = 'today'
        task_name = self._base_name + '.' + tag_key + '.' + output_value + '.' + measurement + '.' + period

        tasks = tasks_api.find_tasks(name=task_name)
        if tasks is None or len(tasks) == 0:
            _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
            right_now = datetime.datetime.now()
            midnight = datetime.datetime.combine(right_now + datetime.timedelta(days=1), datetime.time(0, 0))
            next_midnight = int(midnight.timestamp()) * 1000000000
            cron = '59 23 * * *'
            flux = \
                f'\n' \
                f'from(bucket: "{bucket}")\n' \
                f'  |> range(start: -1d)\n' \
                f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._field == "{period}" and exists r.{tag_key})\n' \
                f'  |> pivot(rowKey:["_time"], columnKey: ["{tag_key}"], valueColumn: "_value")\n' \
                f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {tag_key}: "{output_value}", _field: "{period}", _value: r.{output_value} + r.delta_wh }}))\n' \
                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n' \
                f'  |> map(fn: (r) => ({{ _time: time(v: {next_midnight}), _measurement: "{measurement}", {tag_key}: "{output_value}", _field: "{period}", _value: r._value }}))\n' \
                f'  |> to(bucket: "{bucket}", org: "{organization.name}")\n'

            try:
                tasks_api.create_task_cron(name=task_name, flux=flux, cron=cron, org_id=self._organization.id)
                # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
            except ApiException as e:
                body_dict = json.loads(e.body)
                _LOGGER.error(f"ApiException during task creation in influx_meter_tasks(): {body_dict.get('message', '???')}")
            except Exception as e:
                _LOGGER.error(f"Unexpected exception during task creation in influx_meter_tasks(): {e}")

    async def influx_delta_wh_tasks(self, periods=None):
        """These tasks calculate the change in Wh during a given period."""

        def _delta_wh_worker(period, range):
            measurement = 'energy'
            output_key = '_meter'
            output_value = 'delta_wh'
            task_name = self._base_name + '.' + output_key + '.' + output_value + '.' + measurement + '.' + period

            tasks = tasks_api.find_tasks(name=task_name)
            if tasks is None or len(tasks) == 0:
                _LOGGER.debug(f"InfluxDB task '{task_name}' was not found, creating...")
                flux = \
                    '\n' \
                    f'production_{period} = from(bucket: "multisma2")\n' \
                    f'  |> range(start: {range})\n' \
                    f'  |> filter(fn: (r) => r._measurement == "production" and r._field == "{period}" and r._inverter == "site")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {output_key}: r._inverter, _field: r._field, _value: r._value * 1000.0 }}))\n' \
                    f'  |> yield(name: "production_{period}")\n' \
                    f'\n' \
                    f'consumption_{period} = from(bucket: "cs24")\n' \
                    f'  |> range(start: {range})\n' \
                    f'  |> filter(fn: (r) => r._measurement == "{measurement}" and r._device == "line" and r._field == "{period}")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {output_key}: r._device, _field: r._field, _value: r._value }}))\n' \
                    f'  |> yield(name: "consumption_{period}")\n' \
                    f'\n' \
                    f'union(tables: [production_{period}, consumption_{period}])\n' \
                    f'  |> pivot(rowKey:["_time"], columnKey: ["{output_key}"], valueColumn: "_value")\n' \
                    f'  |> map(fn: (r) => ({{ _time: r._time, _measurement: "{measurement}", {output_key}: "{output_value}", _field: "{period}", _value: r.line - r.site }}))\n' \
                    f'  |> to(bucket: "cs24", org: "{organization.name}")\n'

                try:
                    tasks_api.create_task_every(name=task_name, flux=flux, every=f'{self._sampling_delta_wh}s', organization=organization)
                    # _LOGGER.debug(f"InfluxDB task '{task_name}' was successfully created")
                except ApiException as e:
                    body_dict = json.loads(e.body)
                    _LOGGER.error(f"ApiException during task creation in _delta_wh_worker(): {body_dict.get('message', '???')}")
                except Exception as e:
                    _LOGGER.error(f"Unexpected exception during task creation in _delta_wh_worker(): {e}")

        tasks_api = self._tasks_api
        organization = self._organization

        range = {'today': '-1d', 'month': '-1mo', 'year': '-1y'}
        if periods is None:
            periods = ['today']
        try:
            for period in periods:
                _delta_wh_worker(period=period, range=range.get(period))
        except Exception as e:
            _LOGGER.error(f"Unexpected exception during task creation in influx_delta_wh_tasks(): {e}")
