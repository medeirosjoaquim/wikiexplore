"""Vandalism consumer: ``wiki.vandalism`` → ``suspicious_edit_summary``.

Scores each event with :func:`score_event`; events at/above the configured
threshold are persisted. Persist-then-commit ordering keeps delivery at-least-once
and idempotent (the table has no unique constraint on event_id by design — a
re-delivery simply re-flags the same edit, which is acceptable for a
*detection candidate* feed).
"""
from __future__ import annotations

import datetime as _dt
import logging
import sys
import time

from app.core.config import settings
from app.database.session import SessionLocal
from app.kafka import deserialize, make_consumer, safe_poll
from app.models import SuspiciousEditSummary
from app.services.runtime import configure_logging, install_signal_handlers, should_stop
from app.services.vandalism_logic import score_event

log = logging.getLogger("wikipulse.vandalism")


def _hour_floor(value) -> _dt.datetime:
    if isinstance(value, _dt.datetime):
        dt = value
    elif isinstance(value, str) and value:
        dt = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        dt = _dt.datetime.now(_dt.UTC)
    return dt.replace(minute=0, second=0, microsecond=0)


def _persist(session, batch: list[dict]) -> int:
    rows = []
    for ev in batch:
        result = score_event(ev)
        if result.score < settings.vandalism_threshold:
            continue
        rows.append(
            SuspiciousEditSummary(
                hour=_hour_floor(ev.get("timestamp")),
                language=str(ev.get("language") or "unknown")[:16],
                page_title=str(ev.get("title") or "")[:512],
                username=str(ev.get("user") or "")[:255] or None,
                score=round(result.score, 4),
                reason=",".join(result.reasons)[:255],
                comment=str(ev.get("comment") or "")[:2000],
                source="live",
            )
        )
    if rows:
        session.bulk_save_objects(rows)
        session.commit()
    return len(rows)


def run_vandalism() -> None:
    consumer = make_consumer(
        group_id=f"{settings.kafka_consumer_group}-vandalism",
        topics=[settings.kafka_topic_vandalism],
        auto_offset_reset="latest",
    )
    log.info("vandalism consuming %s (threshold=%.2f)", settings.kafka_topic_vandalism, settings.vandalism_threshold)

    batch: list[dict] = []
    messages: list = []
    last_flush = time.monotonic()
    flagged = 0

    def _flush() -> None:
        nonlocal flagged, last_flush
        if not batch:
            return
        with SessionLocal() as session:
            flagged += _persist(session, batch)
        for m in messages:
            consumer.commit(message=m)
        batch.clear()
        messages.clear()
        last_flush = time.monotonic()
        if flagged:
            log.info("vandalism flagged=%d", flagged)

    while not should_stop():
        msg = safe_poll(consumer)
        if msg is not None:
            payload = deserialize(msg.value())
            if payload is not None:
                batch.append(payload)
                messages.append(msg)
            else:
                consumer.commit(message=msg)
        now = time.monotonic()
        due = now - last_flush >= settings.analytics_flush_interval_s
        if batch and (len(batch) >= settings.vandalism_batch_size or due):
            try:
                _flush()
            except Exception:  # noqa: BLE001
                log.exception("flush failed; offsets not committed, will retry")
                time.sleep(1.0)

    if batch:
        try:
            _flush()
        except Exception:  # noqa: BLE001
            log.exception("final flush failed")
    consumer.close()
    log.info("vandalism stopped flagged=%d", flagged)


def main() -> int:
    configure_logging()
    install_signal_handlers()
    run_vandalism()
    return 0


if __name__ == "__main__":
    sys.exit(main())
