#!/usr/bin/env python3

import logging
import aioesphomeapi
import asyncio

from influxdb_client import InfluxDBClient, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


_LOGGER = logging.getLogger()

_URL = 'http://dundee.local:8086'
_TOKEN = '77pNY1tLywACWa6xXLZTawUO_pq06Th9bIOy-fWsKQLyK4IKNQm8bp-ZtnoF4LNt9CRgT7qm5pO91a83QkIuxA=='
_ORG = 'Parker Lane'
_BUCKET = 'esphome'

_DEFAULT_LOG_FORMAT = '[%(asctime)s] [%(module)s] [%(levelname)s] %(message)s'
_DEFAULT_LOG_LEVEL = 'INFO'


_CLIENT = None
_WRITE_API = None


async def main():
    logging.basicConfig(format=_DEFAULT_LOG_FORMAT, level=_DEFAULT_LOG_LEVEL)

    loop = asyncio.get_running_loop()

    try:
        cli = aioesphomeapi.APIClient(loop, "cs24.local", 6053, "")
        await cli.connect(login=True)

        api_version = cli.api_version
        _LOGGER.info(f"ESPHome API version {api_version.major}.{api_version.minor}")
    except Exception as e:
        _LOGGER.error(f"Unexpected exception connecting to ESPHome: {e}")
        return

    try:
        device_info = await cli.device_info()
        _LOGGER.info(f"Name: '{device_info.name}', model is {device_info.model}")
        _LOGGER.info(f"ESPHome version: {device_info.esphome_version} built on {device_info.compilation_time}")
    except Exception as e:
        _LOGGER.error(f"Unexpected exception accessing device_info(): {e}")
        return

    #_CLIENT = InfluxDBClient(url=_URL, token=_TOKEN, org=_ORG)
    #_WRITE_API = _CLIENT.write_api(write_options=SYNCHRONOUS)

    try:
        sensors, services = await cli.list_entities_services()
        sensor_by_keys = dict((sensor.key, sensor.name) for sensor in sensors)
    except Exception as e:
        _LOGGER.error(f"Unexpected exception accessing list_entities_services(): {e}")
        return

    def cb(state):
        if type(state) == aioesphomeapi.SensorState:
            _LOGGER.info(f"{sensor_by_keys[state.key]}: {state.state}")

    await cli.subscribe_states(cb)


loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(main())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    loop.close()