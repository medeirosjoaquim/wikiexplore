"""Historical analytics endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.database.session import get_db
from app.models import HourlyLanguageStats, HourlyWikiStats

router = APIRouter()


@router.get("/analytics/timeseries")
def timeseries(
    hours: int = Query(24, ge=1, le=720),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.execute(
        select(HourlyWikiStats).order_by(desc(HourlyWikiStats.hour)).limit(hours)
    ).scalars().all()
    rows = list(reversed(rows))
    return [
        {
            "hour": r.hour.isoformat(),
            "total_edits": r.total_edits,
            "minor_edits": r.minor_edits,
            "active_users": r.active_users,
        }
        for r in rows
    ]


@router.get("/analytics/languages")
def languages(
    hours: int = Query(24, ge=1, le=720),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
) -> list[dict]:
    rows = db.execute(
        select(
            HourlyLanguageStats.language,
            func.sum(HourlyLanguageStats.edits).label("edits"),
            func.sum(HourlyLanguageStats.bytes_added).label("bytes_added"),
        )
        .group_by(HourlyLanguageStats.language)
        .order_by(desc("edits"))
        .limit(limit)
    ).all()
    return [
        {"language": r.language, "edits": int(r.edits or 0), "bytes_added": int(r.bytes_added or 0)}
        for r in rows
    ]
