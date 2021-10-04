import aioesphomeapi
import asyncio

async def main():
    """Connect to an ESPHome device and wait for state changes."""
    loop = asyncio.get_running_loop()
    cli = aioesphomeapi.APIClient(loop, "cs24.local", 6053, "")

    await cli.connect(login=True)
    entities = await cli.list_entities_services()
    for entity in entities:
        for sensor in entity:
            object_id = sensor.get('object_id', None)
            name = sensor.get('name', None)
            key = sensor.get('key', None)
            unique_id = sensor.get('unique_id', None)
            unit_of_measurement = sensor.get('unit_of_measurement=', None)
            print(f"'{name}': {key} {unit_of_measurement} {object_id} {unique_id}")
    print()

    def change_callback(state):
        """Print the state changes of the device.."""
        print(state)

    # Subscribe to the state changes
    await cli.subscribe_states(change_callback)

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(main())
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    loop.close()