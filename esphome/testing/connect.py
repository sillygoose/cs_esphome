import aioesphomeapi
import asyncio

async def main():
   """Connect to an ESPHome device and get details."""
   loop = asyncio.get_running_loop()

   # Establish connection
   api = aioesphomeapi.APIClient(loop, "cs24.local", 6053, "")
   await api.connect(login=True)

   # Get API version of the device's firmware
   print(api.api_version)

   # Show device details
   device_info = await api.device_info()
   print(device_info)
   print()

   # List all entities of the device
   entities = await api.list_entities_services()
   print(entities)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
