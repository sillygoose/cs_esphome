import asyncio
import signal


async def pending_doom():
    await asyncio.sleep(10)
    print(">> Cancelling tasks now")
    for task in asyncio.Task.all_tasks():
        task.cancel()

    print(">> Done cancelling tasks")
    asyncio.get_event_loop().stop()


def ask_exit():
    for task in asyncio.Task.all_tasks():
        task.cancel()


async def looping_coro():
    print("Executing coroutine")
    while True:
        try:
            await asyncio.sleep(2.25)
        except asyncio.CancelledError:
            print("Got CancelledError")
            break

        print("Done waiting")

    print("Done executing coroutine")
    asyncio.get_event_loop().stop()


def main():
    loop = asyncio.get_event_loop()
    asyncio.create_task(pending_doom())
    asyncio.create_task(looping_coro())

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, ask_exit)

    loop.run_forever()

    # I had to manually remove the handlers to
    # avoid an exception on BaseEventLoop.__del__
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.remove_signal_handler(sig)


if __name__ == '__main__':
    #asyncio.run(main())
    main()
