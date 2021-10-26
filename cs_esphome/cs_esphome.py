"""Code to interface with 6-channel CircuitSetup energy monitoring hardware."""
# Robust initialization and shutdown code courtesy of
# https://github.com/wbenny/python-graceful-shutdown.git

import logging
import sys
import time
import signal
import asyncio

from delayedints import DelayedKeyboardInterrupt
from circuitsetup import CircuitSetup
import logfiles
from readconfig import read_config

from exceptions import TerminateSignal, NormalCompletion, AbnormalCompletion, FailedInitialization, WatchdogTimer


_LOGGER = logging.getLogger('cs_esphome')


class CS_ESPHome():
    """Class to encapsulate the ESPHome Api and a CircuitSetup energy monitor."""
    def __init__(self, config):
        """Initialize the ESPHome instance."""
        self._config = config
        self._loop = asyncio.new_event_loop()
        self._cs_esphome = None
        signal.signal(signal.SIGTERM, self.catch)
        signal.siginterrupt(signal.SIGTERM, False)

    def catch(self, signum, frame):
        """Handler for SIGTERM signals."""
        _LOGGER.critical("Received SIGTERM signal, forcing shutdown")
        raise TerminateSignal

    def run(self):
        """Code to handle the start(), run(), and stop() interfaces."""
        # ERROR_DELAY might be non-zero when some errors are detected *for now not implemented)
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
        except WatchdogTimer as e:
            _LOGGER.critical(f"Received WatchdogTimer exception: {e}")
            delay = 10
        except AbnormalCompletion:
            _LOGGER.critical("Received AbnormalCompletion exception")
            delay = ERROR_DELAY
        except FailedInitialization:
            # _LOGGER.critical("Received FailedInitialization exception")
            delay = ERROR_DELAY
        except Exception as e:
            _LOGGER.error(f"Unexpected exception caught: {e}")
        finally:
            try:
                with DelayedKeyboardInterrupt():
                    self._stop()
            except KeyboardInterrupt:
                _LOGGER.critical("Received KeyboardInterrupt during shutdown")
            finally:
                if delay > 0:
                    _LOGGER.info(f"CS/ESPHome is delaying restart for {delay} seconds (Docker will restart CS/ESPHome, otherwise exits)")
                    time.sleep(delay)

    async def _astart(self):
        """Asynchronous initialization code."""
        config = self._config.cs_esphome
        self._cs_esphome = CircuitSetup(config=config)
        result = await self._cs_esphome.start()
        if not result:
            raise FailedInitialization

    async def _arun(self):
        """Asynchronous run code."""
        await self._cs_esphome.run()

    async def _astop(self):
        """Asynchronous closing code."""
        _LOGGER.info("Closing CS/ESPHome application")
        if self._cs_esphome:
            await self._cs_esphome.stop()

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
    """Set up and start CS/ESPHome application."""
    logfiles.start()
    try:
        config = read_config()
        if config:
            cs_esphome = CS_ESPHome(config)
            cs_esphome.run()
    except FailedInitialization as e:
        _LOGGER.error(f"{e}")
    except Exception as e:
        _LOGGER.error(f"Unexpected exception: {e}")


if __name__ == "__main__":
    # make sure we can run CS/ESPHome
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 8:
        main()
    else:
        print("python 3.8 or better required")
