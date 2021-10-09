"""Module handling the application and production log files/"""

import os
import sys
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler


_LOGGER = logging.getLogger('cs_esp')

_DEFAULT_LOG_FILE = 'cs_esp'
_DEFAULT_LOG_FORMAT = '[%(asctime)s] [%(module)s] [%(levelname)s] %(message)s'
_DEFAULT_LOG_LEVEL = 'INFO'


def start():
    """Create the application log."""
    log_file = _DEFAULT_LOG_FILE
    log_format = _DEFAULT_LOG_FORMAT
    log_level = _DEFAULT_LOG_LEVEL

    now = datetime.now()
    filename = os.path.expanduser(log_file + "_" + now.strftime("%Y-%m-%d") + ".log")

    # Create the directory if needed
    filename_parts = os.path.split(filename)
    if filename_parts[0] and not os.path.isdir(filename_parts[0]):
        os.mkdir(filename_parts[0])
    filename = os.path.abspath(filename)

    # Change log files at midnight
    handler = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=10)
    handler.suffix = "%Y-%m-%d"
    handler.setLevel(log_level)
    formatter = logging.Formatter(log_format)
    handler.setFormatter(formatter)
    _LOGGER.addHandler(handler)

    # Add some console output for anyone watching
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(logging.Formatter(log_format))
    _LOGGER.addHandler(console_handler)
    _LOGGER.setLevel(log_level)

    # First entry
    _LOGGER.info("Created application log %s", filename)
