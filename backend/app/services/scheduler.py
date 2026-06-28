"""Scheduler: periodic background jobs for the whole platform.

Runs every WikiPulse background job on its own cadence in a single process:

* hourly live-index rollover    — every ``SCHEDULER_HOURLY_INTERVAL_S``
* live distinct reconciliation  — every ``SCHEDULER_RECONCILE_INTERVAL_S``
* 3-hour consolidation          — every ``SCHEDULER_CONSOLIDATE_INTERVAL_S``
* ES live cleanup               — every ``SCHEDULER_CLEANUP_LIVE_INTERVAL_S``
* PG aggregate cleanup          — every ``SCHEDULER_CLEANUP_AGG_INTERVAL_S``

Each task is independent and resilient: a failure in one (e.g. ES briefly
down) is logged and retried on the next tick without affecting the others.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
import time

from app.core.config import settings
from app.database.session import SessionLocal
from app.jobs.cleanup_live_data import run_cleanup as cleanup_live
from app.jobs.cleanup_old_aggregates import run_cleanup as cleanup_agg
from app.jobs.consolidate_3h import run_consolidation
from app.jobs.reconcile_live import run_reconcile
from app.search.aliases import ensure_current_hour_index, get_client
from app.services.runtime import configure_logging, install_signal_handlers, should_stop

log = logging.getLogger("wikipulse.scheduler")


async def _every(name: str, interval_s: int, fn, *args) -> None:
    """Run ``fn(*args)`` every ``interval_s``, tolerating exceptions."""
    log.info("scheduler task '%s' every %ds", name, interval_s)
    while not should_stop():
        start = time.monotonic()
        try:
            await asyncio.to_thread(fn, *args)
        except Exception:  # noqa: BLE001
            log.exception("scheduler task '%s' failed", name)
        # Sleep the remainder, but wake at most every second to notice shutdown.
        deadline = time.monotonic() + max(0.0, interval_s - (time.monotonic() - start))
        while time.monotonic() < deadline and not should_stop():
            await asyncio.sleep(min(1.0, deadline - time.monotonic()))


def _hourly_rollover() -> None:
    es = get_client()
    name = ensure_current_hour_index(es)
    log.debug("hourly rollover -> %s", name)


def _reconcile() -> None:
    with SessionLocal() as session:
        result = run_reconcile(session)
    log.info("reconcile %s", result)


def _consolidate() -> None:
    with SessionLocal() as session:
        result = run_consolidation(session)
    log.info("consolidation %s", result)


def _cleanup_live() -> None:
    result = cleanup_live()
    log.info("live cleanup deleted=%d", len(result.get("deleted", [])))


def _cleanup_agg() -> None:
    with SessionLocal() as session:
        result = cleanup_agg(session)
    log.info("aggregate cleanup %s", result)


async def run_scheduler() -> None:
    tasks = [
        asyncio.create_task(_every("hourly_rollover", settings.scheduler_hourly_interval_s, _hourly_rollover)),
        asyncio.create_task(_every("reconcile", settings.scheduler_reconcile_interval_s, _reconcile)),
        asyncio.create_task(_every("consolidate", settings.scheduler_consolidate_interval_s, _consolidate)),
        asyncio.create_task(_every("cleanup_live", settings.scheduler_cleanup_live_interval_s, _cleanup_live)),
        asyncio.create_task(_every("cleanup_agg", settings.scheduler_cleanup_agg_interval_s, _cleanup_agg)),
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        for t in tasks:
            t.cancel()


def main() -> int:
    configure_logging()
    install_signal_handlers()
    log.info("WikiPulse scheduler starting")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_scheduler())
    return 0


if __name__ == "__main__":
    sys.exit(main())
