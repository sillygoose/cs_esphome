"""Exceptions used in esphome."""

from enum import Enum, auto


class NormalCompletion(Exception):
    """Normal completion, no errors."""


class AbnormalCompletion(Exception):
    """Abnormal completion, error or exception detected."""


class FailedInitialization(Exception):
    """esphome initialization failed."""


class WatchdogTimer(Exception):
    """Watchdog timer tripped."""


class InfluxDBWriteError(Exception):
    """Watchdog timer tripped."""


class InfluxDBFormatError(Exception):
    """Illegal or unsupport database output format."""


class InfluxDBInitializationError(Exception):
    """InfluxDB is not properly initialized."""


class TerminateSignal(Exception):
    """SIGTERM."""
