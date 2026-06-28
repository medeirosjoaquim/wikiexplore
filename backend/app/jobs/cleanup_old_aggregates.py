"""Delete old PostgreSQL hourly aggregates past their retention window."""
from __future__ import annotations

import datetime as _dt
import sys

from sqlalchemy import delete
from sqlalchemy.orm import Session

from app.core.config import settings
from app.database.session import SessionLocal
from app.models import (
    ConsolidatedWindow,
    HourlyLanguageStats,
    HourlyPageStats,
    HourlyUserStats,
    HourlyWikiStats,
    SuspiciousEditSummary,
)


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


def run_cleanup(session: Session) -> dict:
    now = _utc_now()
    hourly_cutoff = now - _dt.timedelta(days=settings.hourly_retention_days)
    cons_cutoff = now - _dt.timedelta(days=settings.consolidated_retention_days)

    counts: dict[str, int | str] = {}
    for model in (
        HourlyWikiStats,
        HourlyLanguageStats,
        HourlyPageStats,
        HourlyUserStats,
        SuspiciousEditSummary,
    ):
        res = session.execute(delete(model).where(model.hour < hourly_cutoff))
        counts[model.__tablename__] = res.rowcount or 0

    res = session.execute(
        delete(ConsolidatedWindow).where(ConsolidatedWindow.window_start < cons_cutoff)
    )
    counts[ConsolidatedWindow.__tablename__] = res.rowcount or 0
    session.commit()
    counts["hourly_cutoff"] = hourly_cutoff.isoformat()
    counts["consolidated_cutoff"] = cons_cutoff.isoformat()
    return counts


def main() -> int:
    with SessionLocal() as session:
        result = run_cleanup(session)
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
