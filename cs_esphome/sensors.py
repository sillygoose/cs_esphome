"""Code to work with sensor data."""

import logging


_LOGGER = logging.getLogger('cs_esphome')


def parse_by_location(sensors):
    """Returns a dictionary of devices organized by the location."""
    location_directory = {}
    for sensor in sensors.values():
        location = sensor.get('location', None)
        integrate = sensor.get('integrate', None)
        if location is None or integrate is None:
            continue
        if location and integrate:
            device = sensor.get('device', None)
            measurement = sensor.get('measurement', None)
            locations = location_directory.get(location, None)
            if not locations:
                location_directory[location] = [{'device': device, 'measurement': measurement}]
            else:
                entry = location_directory[location]
                location_measurement = entry[0].get('measurement', None)
                if location_measurement != measurement:
                    _LOGGER.error("All measurements in a location must be the same!")
                    return {}
                entry.append({'device': device, 'measurement': measurement})
                location_directory[location] = entry
    return location_directory


def parse_by_integration(sensors):
    """Returns a list of devices that can be integrated."""
    can_integrate = []
    for sensor in sensors.values():
        if sensor.get('integrate'):
            can_integrate.append(sensor)
    return can_integrate


def parse_sensors(yaml, entities):
    """Combine the sensors from the ESPHome device with the YAML file descriptions."""
    sensors_by_name = {}
    sensors_by_key = {}

    keys_by_name = dict((sensor.name, sensor.key) for sensor in entities)
    units_by_name = dict((sensor.name, sensor.unit_of_measurement) for sensor in entities)
    decimals_by_name = dict((sensor.name, sensor.accuracy_decimals) for sensor in entities)

    try:
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

    except Exception as e:
        _LOGGER.error(f"Unexpected exception in parse_sensors(): {e}")

    return sensors_by_name, sensors_by_key
