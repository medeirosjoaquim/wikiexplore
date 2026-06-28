"""Analytics consumer: ``wiki.analytics`` → PostgreSQL hourly aggregates.

Batches messages, applies additive increments via the repository, commits the
DB transaction, then commits Kafka offsets — DB-first ordering means a crash
between the two replays events (idempotent increments) rather than losing them.

Tracing: each event continues its origin trace via the Kafka headers while it
is queued; the batch upsert is one ``analytics.pg_upsert`` span (per-table
child spans come from SQLAlchemy instrumentation).
"""
from __future__ import annotations

import logging
import sys
import time

from app.core.config import settings
from app.database.session import SessionLocal
from app.kafka import deserialize, make_consumer, safe_poll
from app.observability import (
    ANALYTICS_APPLIED,
    STAGE_SECONDS,
    UPSERT_BATCH,
    extract_context,
    setup_tracing,
    span,
    start_metrics_server,
)
from app.repositories import apply_events
from app.services.runtime import configure_logging, install_signal_handlers, should_stop

log = logging.getLogger("wikipulse.analytics")


def run_analytics() -> None:
    setup_tracing("wikipulse-analytics", "analytics")
    start_metrics_server()
    consumer = make_consumer(
        group_id=f"{settings.kafka_consumer_group}-analytics",
        topics=[settings.kafka_topic_analytics],
        auto_offset_reset="latest",
    )
    log.info("analytics consuming %s -> PostgreSQL", settings.kafka_topic_analytics)

    batch: list[dict] = []
    messages: list = []
    last_flush = time.monotonic()
    applied = 0

    def _flush() -> None:
        nonlocal applied, last_flush
        if not batch:
            return
        UPSERT_BATCH.labels(table="hourly_*").observe(len(batch))
        with STAGE_SECONDS.labels(stage="pg_upsert").time(), SessionLocal() as session:
            count = apply_events(session, batch, source="live")
        ANALYTICS_APPLIED.labels(outcome="applied").inc(count)
        applied += count
        for m in messages:
            consumer.commit(message=m)
        batch.clear()
        messages.clear()
        last_flush = time.monotonic()
        log.info("analytics applied=%d", applied)

    while not should_stop():
        msg = safe_poll(consumer)
        if msg is not None:
            payload = deserialize(msg.value())
            if payload is not None:
                # Continue this event's trace while it waits in the batch.
                with span(
                    "analytics.queue_event",
                    context=extract_context(msg.headers()),
                    event_id=payload.get("event_id"),
                    language=payload.get("language"),
                ):
                    batch.append(payload)
                messages.append(msg)
            else:
                consumer.commit(message=msg)
        now = time.monotonic()
        due = now - last_flush >= settings.analytics_flush_interval_s
        if batch and (len(batch) >= settings.analytics_batch_size or due):
            try:
                with span("analytics.pg_upsert", events=len(batch)):
                    _flush()
            except Exception:  # noqa: BLE001
                log.exception("flush failed; offsets not committed, will retry")
                time.sleep(1.0)

    if batch:
        try:
            with span("analytics.pg_upsert", events=len(batch)):
                _flush()
        except Exception:  # noqa: BLE001
            log.exception("final flush failed")
    consumer.close()
    log.info("analytics stopped applied=%d", applied)


def main() -> int:
    configure_logging()
    install_signal_handlers()
    run_analytics()
    return 0


if __name__ == "__main__":
    sys.exit(main())
