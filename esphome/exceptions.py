"""Exceptions used in esphome."""

from enum import Enum, auto


class NormalCompletion(Exception):
    """Normal completion, no errors."""


class AbnormalCompletion(Exception):
    """Abnormal completion, error or exception detected."""


class FailedInitialization(Exception):
    """esphome initialization failed."""


class TerminateSignal(Exception):
    """SIGTERM."""
