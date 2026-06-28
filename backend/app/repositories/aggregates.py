"""Aggregate repository: increment PostgreSQL hourly aggregates.

Pure data-access layer — no Kafka, no business decisions. The analytics
consumer calls :func:`apply_events` with a batch of canonical event dicts and
this module turns them into idempotent ``INSERT ... ON CONFLICT DO UPDATE``
increments across the four hourly tables.

Distinct counts (active_users, active_pages, distinct_languages, distinct_users)
are intentionally NOT maintained here: they are reconciled from Elasticsearch
cardinality aggregations by the scheduler (see ``app.jobs.reconcile_live``).
Keeping them out of the hot path avoids per-edit distinct tracking, which would
violate ADR-003 (PG stores aggregates, not raw events).
"""
from __future__ import annotations

import datetime as _dt

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.models import (
    HourlyLanguageStats,
    HourlyPageStats,
    HourlyUserStats,
    HourlyWikiStats,
)


def _hour_floor(ts: _dt.datetime) -> _dt.datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def _coerce_ts(value) -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        return value
    if isinstance(value, str) and value:
        return _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return _dt.datetime.now(_dt.UTC)


def build_rows(events: list[dict], source: str = "live") -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    """Aggregate canonical event dicts into four deduplicated upsert batches.

    Pure function (no DB) so it can be unit-tested without PostgreSQL. Each
    batch is summed per unique key so a single multi-row ``INSERT ... ON
    CONFLICT DO UPDATE`` never targets the same row twice within one statement
    (which Postgres rejects as a cardinality violation). Returns
    ``(wiki_rows, lang_rows, page_rows, user_rows)``.
    """
    wiki: dict[tuple, dict] = {}
    lang: dict[tuple, dict] = {}
    page: dict[tuple, dict] = {}
    user: dict[tuple, dict] = {}

    for ev in events:
        hour = _hour_floor(_coerce_ts(ev.get("timestamp")))
        minor = 1 if ev.get("minor") else 0
        added = max(int(ev.get("bytes_added") or 0), 0)
        removed = max(int(ev.get("bytes_removed") or 0), 0)
        language = str(ev.get("language") or "unknown")[:16]
        username = str(ev.get("user") or "anonymous")[:255]
        title = str(ev.get("title") or "")[:512]
        page_id = int(ev.get("page_id") or 0) or None
        is_bot = bool(ev.get("bot"))

        w = wiki.setdefault((hour, source), {"hour": hour, "source": source, "total_edits": 0, "minor_edits": 0, "total_bytes_added": 0, "total_bytes_removed": 0})
        w["total_edits"] += 1
        w["minor_edits"] += minor
        w["total_bytes_added"] += added
        w["total_bytes_removed"] += removed

        lk = lang.setdefault((hour, language, source), {"hour": hour, "language": language, "source": source, "edits": 0, "minor_edits": 0, "bytes_added": 0, "bytes_removed": 0})
        lk["edits"] += 1
        lk["minor_edits"] += minor
        lk["bytes_added"] += added
        lk["bytes_removed"] += removed

        pg = page.setdefault((hour, language, title, source), {"hour": hour, "language": language, "page_title": title, "page_id": page_id, "source": source, "edits": 0, "bytes_added": 0, "bytes_removed": 0})
        pg["edits"] += 1
        pg["bytes_added"] += added
        pg["bytes_removed"] += removed
        pg["page_id"] = page_id  # keep the latest non-null page_id

        uk = user.setdefault((hour, username, source), {"hour": hour, "username": username, "source": source, "edits": 0, "minor_edits": 0, "bytes_added": 0, "bytes_removed": 0, "is_bot": is_bot})
        uk["edits"] += 1
        uk["minor_edits"] += minor
        uk["bytes_added"] += added
        uk["bytes_removed"] += removed
        uk["is_bot"] = is_bot

    return list(wiki.values()), list(lang.values()), list(page.values()), list(user.values())


def apply_events(session: Session, events: list[dict], source: str = "live") -> int:
    """Apply a batch of events as additive increments. Returns events applied.

    Uses one ``ON CONFLICT DO UPDATE`` upsert per table so a batch of N events
    is 4 round-trips, not 4N.
    """
    if not events:
        return 0

    wiki_rows, lang_rows, page_rows, user_rows = build_rows(events, source)
    _upsert_wiki(session, wiki_rows)
    _upsert_lang(session, lang_rows)
    _upsert_page(session, page_rows)
    _upsert_user(session, user_rows)
    session.commit()
    return len(events)


def _upsert_wiki(session: Session, rows: list[dict]) -> None:
    stmt = insert(HourlyWikiStats).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_hourly_wiki_stats_hour_source",
        set_={
            "total_edits": HourlyWikiStats.total_edits + stmt.excluded.total_edits,
            "minor_edits": HourlyWikiStats.minor_edits + stmt.excluded.minor_edits,
            "total_bytes_added": HourlyWikiStats.total_bytes_added + stmt.excluded.total_bytes_added,
            "total_bytes_removed": HourlyWikiStats.total_bytes_removed + stmt.excluded.total_bytes_removed,
        },
    )
    session.execute(stmt)


def _upsert_lang(session: Session, rows: list[dict]) -> None:
    stmt = insert(HourlyLanguageStats).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_hourly_language_stats_hour_lang_source",
        set_={
            "edits": HourlyLanguageStats.edits + stmt.excluded.edits,
            "minor_edits": HourlyLanguageStats.minor_edits + stmt.excluded.minor_edits,
            "bytes_added": HourlyLanguageStats.bytes_added + stmt.excluded.bytes_added,
            "bytes_removed": HourlyLanguageStats.bytes_removed + stmt.excluded.bytes_removed,
        },
    )
    session.execute(stmt)


def _upsert_page(session: Session, rows: list[dict]) -> None:
    stmt = insert(HourlyPageStats).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_hourly_page_stats_hour_lang_page_source",
        set_={
            "edits": HourlyPageStats.edits + stmt.excluded.edits,
            "bytes_added": HourlyPageStats.bytes_added + stmt.excluded.bytes_added,
            "bytes_removed": HourlyPageStats.bytes_removed + stmt.excluded.bytes_removed,
            "page_id": stmt.excluded.page_id,
        },
    )
    session.execute(stmt)


def _upsert_user(session: Session, rows: list[dict]) -> None:
    stmt = insert(HourlyUserStats).values(rows)
    stmt = stmt.on_conflict_do_update(
        constraint="uq_hourly_user_stats_hour_user_source",
        set_={
            "edits": HourlyUserStats.edits + stmt.excluded.edits,
            "minor_edits": HourlyUserStats.minor_edits + stmt.excluded.minor_edits,
            "bytes_added": HourlyUserStats.bytes_added + stmt.excluded.bytes_added,
            "bytes_removed": HourlyUserStats.bytes_removed + stmt.excluded.bytes_removed,
        },
    )
    session.execute(stmt)
