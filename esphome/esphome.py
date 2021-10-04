"""Code to interface with 6-channel CircuitSetup energy monitoring hardware."""
# Robust initialization and shutdown code courtesy of
# https://github.com/wbenny/python-graceful-shutdown.git

import logging
import sys
import os
import time
import signal
from readconfig import read_config

import asyncio
import aiohttp

from delayedints import DelayedKeyboardInterrupt
from circuitsetup import CircuitSetup
import version
import logfiles

from exceptions import TerminateSignal, NormalCompletion, AbnormalCompletion, FailedInitialization


_LOGGER = logging.getLogger('esphome')


class ESPHome():

    def __init__(self, config):
        """Initialize the ESPHome instance."""
        self._config = config
        self._loop = asyncio.new_event_loop()
        self._cs24 = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)

    def catch(self, signum, frame):
        """Handler for SIGTERM signals."""
        _LOGGER.critical("Received SIGTERM signal, forcing shutdown")
        raise TerminateSignal

    def run(self):
        """Code to handle the start(), run(), and stop() interfaces."""
        # ERROR_DELAY might be non-zero when SMA errors are detected *for now not implemented)
        ERROR_DELAY = 0
        delay = 0
        try:
            try:
                with DelayedKeyboardInterrupt():
                    self._start()
            except KeyboardInterrupt:
                _LOGGER.critical("Received KeyboardInterrupt during startup")
                raise

            self._run()
            raise NormalCompletion

        except (KeyboardInterrupt, NormalCompletion, TerminateSignal):
            pass
        except AbnormalCompletion:
            # _LOGGER.critical("Received AbnormalCompletion exception detected")
            delay = ERROR_DELAY
        except FailedInitialization:
            # _LOGGER.critical("Received FailedInitialization exception detected")
            delay = ERROR_DELAY
        except Exception as e:
            _LOGGER.error(f"Unexpected exception caught: {e}")
            delay = 0
        finally:
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                _LOGGER.critical("Received KeyboardInterrupt during shutdown")
            finally:
                if delay > 0:
                    print(
                        f"esphome is delaying restart for {delay} seconds (Docker will restart esphome, otherwise exits)")
                    time.sleep(delay)

    async def _astart(self):
        """Asynchronous initialization code."""
        config = self._config.esphome
        self._cs24 = CircuitSetup(config=config)
        result = await self._cs24.start()
        if not result:
            raise FailedInitialization

    async def _arun(self):
        """Asynchronous run code."""
        await self._cs24.run()

    async def _astop(self):
        """Asynchronous closing code."""
        _LOGGER.info("Closing esphome application")
        if self._cs24:
            await self._cs24.stop()

    def _start(self):
        """Initialize everything prior to running."""
        self._loop.run_until_complete(self._astart())

    def _run(self):
        """Run the application."""
        self._loop.run_until_complete(self._arun())

    def _stop(self):
        """Cleanup after running."""
        self._loop.run_until_complete(self._astop())


def main():
    """Set up and start esphome."""

    try:
        config = read_config(checking=False)
    except FailedInitialization as e:
        print(f"{e}")
        return

    logfiles.start(config)
    _LOGGER.info(f"esphome energy collection utility {version.get_version()}, PID is {os.getpid()}")

    try:
        config = read_config(checking=True)
        if config:
            esphome = ESPHome(config)
            esphome.run()
    except FailedInitialization as e:
        _LOGGER.error(f"{e}")
    except Exception as e:
        _LOGGER.error(f"Unexpected exception: {e}")


if __name__ == "__main__":
    # make sure we can run esphome
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 8:
        main()
    else:
        print("python 3.8 or better required")
