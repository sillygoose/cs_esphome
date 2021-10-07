"""Code to work with sensor data."""

import logging


_LOGGER = logging.getLogger('esphome')


def parse_by_location(sensors):
    location_directory = {}
    for sensor in sensors.values():
        location = sensor.get('location', None)
        if location:
            locations = location_directory.get(location, None)
            if not locations:
                location_directory[location] = [sensor]
            else:
                entry = location_directory[location]
                entry.append(sensor)
                location_directory[location] = entry

    return location_directory

def parse_by_integration(sensors):
    integratable = []
    for sensor in sensors.values():
        if sensor.get('integrate'):
            integratable.append(sensor)
    return integratable

def parse_sensors(yaml, entities):
    sensors_by_name = {}
    sensors_by_key = {}
    keys_by_name = dict((sensor.name, sensor.key) for sensor in entities)
    units_by_name = dict((sensor.name, sensor.unit_of_measurement) for sensor in entities)
    decimals_by_name = dict((sensor.name, sensor.accuracy_decimals) for sensor in entities)
    for entry in yaml:
        for details in entry.values():
            enable = details.get('enable', True)
            sensor_name = details.get('sensor_name', None)
            key = keys_by_name.get(sensor_name, None)
            if key and enable:
                data = {
                    'sensor_name': details.get('sensor_name', None),
                    'display_name': details.get('display_name', None),
                    'unit': units_by_name.get(sensor_name, None),
                    'key': keys_by_name.get(sensor_name, None),
                    'precision': decimals_by_name.get(sensor_name, None),
                    'measurement': details.get('measurement', None),
                    'device': details.get('device', None),
                    'location': details.get('location', None),
                    'integrate': details.get('integrate', False),
                }
                sensors_by_name[sensor_name] = data
                sensors_by_key[key] = data

    return sensors_by_name, sensors_by_key
