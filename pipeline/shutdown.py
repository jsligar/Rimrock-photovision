"""Shared graceful-shutdown event for the pipeline.

All phases should check ``is_requested()`` in their main loop.
The API stop endpoint calls ``request()``.  The phase runner calls
``clear()`` before starting each new phase so a previous stop request
does not bleed into the next run.
"""

import threading

_shutdown_event = threading.Event()


def request() -> None:
    """Signal all running phases to stop after their current unit of work."""
    _shutdown_event.set()


def clear() -> None:
    """Clear the shutdown flag before starting a new phase."""
    _shutdown_event.clear()


def is_requested() -> bool:
    """Return True if a stop has been requested."""
    return _shutdown_event.is_set()
