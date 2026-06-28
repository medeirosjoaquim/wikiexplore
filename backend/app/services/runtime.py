"""Shared helpers for long-running consumer services."""
from __future__ import annotations

import contextlib
import logging
import signal
import threading

log = logging.getLogger("wikipulse.runtime")

# A simple Event set by the signal handler so poll loops can drain and exit.
_stop = threading.Event()


def request_stop(*_: object) -> None:
    log.info("stop requested")
    _stop.set()


def should_stop() -> bool:
    return _stop.is_set()

def install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(ValueError, OSError):
            signal.signal(sig, request_stop)


def reset_for_tests() -> None:
    """Clear the stop flag so unit tests can reuse the module."""
    _stop.clear()


def configure_logging() -> None:
    from app.core.config import settings

    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
