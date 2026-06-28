"""Reconcile live distinct counts from Elasticsearch into PostgreSQL.

The analytics consumer maintains additive counters in real time but cannot
maintain distinct counts (active users/pages/languages) without storing raw
rows — which ADR-003 forbids. Elasticsearch already holds the live detail, so
this job periodically pulls cardinality aggregations for the last few hours
and writes the authoritative distinct counts back into the hourly tables.

Failure of Elasticsearch is tolerated: the job no-ops and additive counters
remain live. The dashboard lags only on distinct metrics until ES recovers.
"""
from __future__ import annotations

import datetime as _dt
import logging

from sqlalchemy import update
from sqlalchemy.orm import Session

from app.core.config import settings
from app.models import HourlyLanguageStats, HourlyWikiStats
from app.search.aliases import get_client

log = logging.getLogger("wikipulse.reconcile")


def _hour_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _query_hour(es, hour: _dt.datetime) -> tuple[int, int, int, dict[str, int]]:
    """Return (distinct_users, distinct_pages, distinct_languages, {lang: users})."""
    start = hour
    end = hour + _dt.timedelta(hours=1)
    body = {
        "size": 0,
        "query": {"range": {"@timestamp": {"gte": start.isoformat(), "lt": end.isoformat()}}},
        "aggs": {
            "users": {"cardinality": {"field": "user.keyword"}},
            "pages": {"cardinality": {"field": "title.keyword"}},
            "langs": {"cardinality": {"field": "language"}},
            "by_lang": {
                "terms": {"field": "language", "size": 100},
                "aggs": {"users": {"cardinality": {"field": "user.keyword"}}},
            },
        },
    }
    resp = es.search(index=settings.es_live_read_alias, body=body)
    aggs = resp.get("aggregations", {})
    users = int(aggs.get("users", {}).get("value", 0))
    pages = int(aggs.get("pages", {}).get("value", 0))
    langs = int(aggs.get("langs", {}).get("value", 0))
    per_lang = {
        b["key"]: int(b.get("users", {}).get("value", 0))
        for b in aggs.get("by_lang", {}).get("buckets", [])
    }
    return users, pages, langs, per_lang


def _write_hour(session: Session, hour: _dt.datetime, users: int, pages: int, langs: int, per_lang: dict[str, int]) -> None:
    session.execute(
        update(HourlyWikiStats)
        .where(HourlyWikiStats.hour == hour, HourlyWikiStats.source == "live")
        .values(active_users=users, active_pages=pages, distinct_languages=langs)
    )
    for language, lang_users in per_lang.items():
        session.execute(
            update(HourlyLanguageStats)
            .where(
                HourlyLanguageStats.hour == hour,
                HourlyLanguageStats.language == language,
                HourlyLanguageStats.source == "live",
            )
            .values(active_users=lang_users)
        )
    session.commit()


def run_reconcile(session: Session, es=None, hours: int | None = None) -> dict:
    es = es or get_client()
    lookback = hours or settings.reconcile_hours
    now = _hour_floor(_dt.datetime.now(_dt.UTC))
    reconciled = 0
    skipped: list[str] = []
    for h in range(lookback):
        hour = now - _dt.timedelta(hours=h)
        try:
            users, pages, langs, per_lang = _query_hour(es, hour)
        except Exception as exc:  # noqa: BLE001
            skipped.append(f"{hour.isoformat()}: {exc}")
            continue
        if users == 0 and pages == 0:
            continue  # nothing indexed for this hour yet
        _write_hour(session, hour, users, pages, langs, per_lang)
        reconciled += 1
    if skipped:
        log.warning("reconcile skipped %d hour(s): %s", len(skipped), skipped[0])
    return {"hours_reconciled": reconciled, "lookback": lookback, "skipped": len(skipped)}
