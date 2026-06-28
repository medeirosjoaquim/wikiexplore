"""FastAPI application factory and routes."""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import desc, func, select
from sqlalchemy.orm import Session

from app.api import analytics, live, websocket
from app.database.session import get_db
from app.models import (
    ConsolidatedWindow,
    HourlyLanguageStats,
    HourlyPageStats,
    HourlyUserStats,
    HourlyWikiStats,
    SuspiciousEditSummary,
)
from app.services import live_broadcast
from app.services.health import build_health_report


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await live_broadcast.start()
    try:
        yield
    finally:
        await live_broadcast.stop()

app = FastAPI(
    title="WikiPulse API",
    version="0.1.0",
    description="Real-time Wikipedia edit monitoring, analytics, and vandalism detection.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analytics.router, prefix="/api", tags=["analytics"])
app.include_router(live.router, prefix="/api", tags=["live"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])


@app.get("/")
def root() -> dict:
    return {"name": "WikiPulse API", "version": app.version, "docs": "/docs"}


@app.get("/health", include_in_schema=True)
def health(db: Session = Depends(get_db)) -> JSONResponse:
    report = build_health_report(db)
    status_code = 200 if report["status"] == "healthy" else 503
    return JSONResponse(status_code=status_code, content=report)


@app.get("/api/overview")
def overview(db: Session = Depends(get_db)) -> dict:
    """Latest hourly aggregate for the headline dashboard tiles."""
    row = db.execute(
        select(HourlyWikiStats)
        .order_by(desc(HourlyWikiStats.hour))
        .limit(1)
    ).scalar_one_or_none()
    if row is None:
        return {"hour": None, "total_edits": 0}
    return {
        "hour": row.hour.isoformat(),
        "total_edits": row.total_edits,
        "minor_edits": row.minor_edits,
        "total_bytes_added": row.total_bytes_added,
        "total_bytes_removed": row.total_bytes_removed,
        "active_users": row.active_users,
        "active_pages": row.active_pages,
        "distinct_languages": row.distinct_languages,
        "source": row.source,
    }


@app.get("/api/top-languages")
def top_languages(limit: int = Query(10, ge=1, le=50), db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(
        select(
            HourlyLanguageStats.language,
            func.sum(HourlyLanguageStats.edits).label("edits"),
        )
        .group_by(HourlyLanguageStats.language)
        .order_by(desc("edits"))
        .limit(limit)
    ).all()
    return [{"language": r.language, "edits": int(r.edits or 0)} for r in rows]


@app.get("/api/top-pages")
def top_pages(limit: int = Query(20, ge=1, le=100), db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(
        select(
            HourlyPageStats.language,
            HourlyPageStats.page_title,
            func.sum(HourlyPageStats.edits).label("edits"),
        )
        .group_by(HourlyPageStats.language, HourlyPageStats.page_title)
        .order_by(desc("edits"))
        .limit(limit)
    ).all()
    return [
        {"language": r.language, "page_title": r.page_title, "edits": int(r.edits or 0)}
        for r in rows
    ]


@app.get("/api/top-users")
def top_users(limit: int = Query(20, ge=1, le=100), db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(
        select(
            HourlyUserStats.username,
            HourlyUserStats.is_bot,
            func.sum(HourlyUserStats.edits).label("edits"),
        )
        .group_by(HourlyUserStats.username, HourlyUserStats.is_bot)
        .order_by(desc("edits"))
        .limit(limit)
    ).all()
    return [
        {"username": r.username, "is_bot": bool(r.is_bot), "edits": int(r.edits or 0)}
        for r in rows
    ]


@app.get("/api/suspicious")
def suspicious(limit: int = Query(50, ge=1, le=500), db: Session = Depends(get_db)) -> list[dict]:
    rows = db.execute(
        select(SuspiciousEditSummary)
        .order_by(desc(SuspiciousEditSummary.detected_at))
        .limit(limit)
    ).scalars().all()
    return [
        {
            "id": r.id,
            "hour": r.hour.isoformat() if r.hour else None,
            "detected_at": r.detected_at.isoformat() if r.detected_at else None,
            "language": r.language,
            "page_title": r.page_title,
            "username": r.username,
            "score": r.score,
            "reason": r.reason,
            "source": r.source,
        }
        for r in rows
    ]

@app.get("/api/system")
def system(db: Session = Depends(get_db)) -> dict:
    """Operational status: consolidation progress, retention, and storage usage."""
    from app.core.config import settings

    last_window = db.execute(
        select(ConsolidatedWindow).order_by(desc(ConsolidatedWindow.window_end)).limit(1)
    ).scalar_one_or_none()
    consolidation = {
        "last_window_start": last_window.window_start.isoformat() if last_window else None,
        "last_window_end": last_window.window_end.isoformat() if last_window else None,
        "last_status": last_window.status if last_window else None,
        "last_completed_at": (
            last_window.completed_at.isoformat() if last_window and last_window.completed_at else None
        ),
        "last_rows_consolidated": last_window.rows_consolidated if last_window else 0,
    }

    def _count(model) -> int:
        return int(db.execute(select(func.count()).select_from(model)).scalar() or 0)

    storage = {
        "hourly_wiki_stats": _count(HourlyWikiStats),
        "hourly_language_stats": _count(HourlyLanguageStats),
        "hourly_page_stats": _count(HourlyPageStats),
        "hourly_user_stats": _count(HourlyUserStats),
        "suspicious_edit_summary": _count(SuspiciousEditSummary),
        "consolidated_windows": _count(ConsolidatedWindow),
    }

    return {
        "consolidation": consolidation,
        "retention": {
            "hourly_days": settings.hourly_retention_days,
            "consolidated_days": settings.consolidated_retention_days,
        },
        "storage": storage,
        "live_retention_hours": settings.live_retention_hours,
        "consolidation_window_hours": settings.consolidation_window_hours,
    }
