"""Exceptions used in CS/ESPHome."""

from enum import Enum, auto


class NormalCompletion(Exception):
    """Normal completion, no errors."""


class AbnormalCompletion(Exception):
    """Abnormal completion, error or exception detected."""


class FailedInitialization(Exception):
    """CS/ESPHome initialization failed."""


class WatchdogTimer(Exception):
    """Watchdog timer tripped."""


class InfluxDBWriteError(Exception):
    """InfluxDB write error."""


class InfluxDBQueryError(Exception):
    """InfluxDB query error."""


class InfluxDBBucketError(Exception):
    """InfluxDB bucket error."""


class InfluxDBFormatError(Exception):
    """Illegal or unsupported database output format."""


class InfluxDBInitializationError(Exception):
    """InfluxDB is not properly initialized."""


class InternalError(Exception):
    """Unexpected/inconsistant state."""


class TerminateSignal(Exception):
    """SIGTERM."""
