"""Utility helpers for the tracking application."""

from datetime import datetime


def utc_now() -> datetime:
    """Return the current UTC time."""

    return datetime.utcnow()
