"""3-hour consolidation job.

Records the completion of a consolidation window and accounts for the raw
events it covered. The authoritative PostgreSQL aggregates are maintained in
real time by the analytics consumer; this job is the **lifecycle boundary**
per ADR-004: after a window is marked completed, its Elasticsearch live data
is eligible for cleanup (see ``cleanup_live_data``).

``rows_consolidated`` is the count of live events that fell in the window,
pulled from Elasticsearch when available (defensive: 0 if ES is unreachable).
Idempotent per (window_start, window_end).
"""
from __future__ import annotations

import argparse
import datetime as _dt
import sys

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.session import SessionLocal
from app.models import ConsolidatedWindow


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


def _hour_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def latest_completed_window(now: _dt.datetime | None = None) -> tuple[_dt.datetime, _dt.datetime]:
    now = _hour_floor(now or _utc_now())
    window_end = now
    window_start = window_end - _dt.timedelta(hours=settings.consolidation_window_hours)
    return window_start, window_end


def _count_window_events(window_start: _dt.datetime, window_end: _dt.datetime) -> int:
    """Count live ES events in [window_start, window_end). Defensive: 0 on error."""
    try:
        from app.search.aliases import get_client

        es = get_client()
        resp = es.count(
            index=settings.es_live_read_alias,
            body={
                "query": {
                    "range": {
                        "@timestamp": {
                            "gte": window_start.isoformat(),
                            "lt": window_end.isoformat(),
                        }
                    }
                }
            },
        )
        return int(resp.get("count", 0))
    except Exception:  # noqa: BLE001
        return 0


def run_consolidation(
    session: Session,
    window_start: _dt.datetime | None = None,
    window_end: _dt.datetime | None = None,
    dry_run: bool = False,
) -> dict:
    if window_start is None or window_end is None:
        window_start, window_end = latest_completed_window()

    rows_in_window = _count_window_events(window_start, window_end)

    existing = session.execute(
        select(ConsolidatedWindow).where(
            ConsolidatedWindow.window_start == window_start,
            ConsolidatedWindow.window_end == window_end,
        )
    ).scalar_one_or_none()

    if dry_run:
        return {
            "dry_run": True,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "already_exists": existing is not None,
            "rows_in_window": rows_in_window,
        }

    if existing is not None:
        existing.status = "completed"
        existing.completed_at = _utc_now()
        existing.rows_consolidated = rows_in_window
        existing.error = None
    else:
        existing = ConsolidatedWindow(
            window_start=window_start,
            window_end=window_end,
            status="completed",
            completed_at=_utc_now(),
            rows_consolidated=rows_in_window,
        )
        session.add(existing)
    session.commit()
    return {
        "dry_run": False,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "status": existing.status,
        "rows_consolidated": existing.rows_consolidated,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="3h consolidation")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--window-start", default=None, help="ISO datetime (UTC)")
    args = parser.parse_args()

    window_start: _dt.datetime | None = None
    if args.window_start:
        window_start = _dt.datetime.fromisoformat(args.window_start)

    with SessionLocal() as session:
        result = run_consolidation(session, window_start=window_start, dry_run=args.dry_run)
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
