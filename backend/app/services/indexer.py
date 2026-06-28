"""ES indexer consumer: ``wiki.index`` → Elasticsearch live index.

Batches messages, ensures the current hourly index exists, bulk-indexes to the
``wiki-live-events-write`` alias, and only then commits offsets — so a crash
before commit means events are re-delivered and re-indexed idempotently
(keyed on ``event_id``).

Tracing: each message's producer/demux trace is continued via the W3C context
in its Kafka headers, so every indexed event links back to its origin span; the
bulk write itself is one ``indexer.es_bulk`` span.
"""
from __future__ import annotations

import logging
import sys
import time

from app.core.config import settings
from app.kafka import deserialize, make_consumer, safe_poll
from app.observability import (
    BULK_INDEX_BATCH,
    INDEXER_EVENTS,
    STAGE_SECONDS,
    extract_context,
    setup_tracing,
    span,
    start_metrics_server,
)
from app.search.aliases import get_client
from app.search.writer import index_events
from app.services.runtime import configure_logging, install_signal_handlers, should_stop

log = logging.getLogger("wikipulse.indexer")


def _flush_batch(es, consumer, messages: list) -> tuple[int, int]:
    """Enrich + bulk-index one batch, committing offsets only on success."""
    from app.events import enrich_for_es

    # Continue each event's distributed trace while building the ES documents.
    docs: list[dict] = []
    for m in messages:
        payload = deserialize(m.value())
        if payload is None:
            continue
        ctx = extract_context(m.headers())
        with span(
            "indexer.index_event",
            context=ctx,
            event_id=payload.get("event_id"),
            language=payload.get("language"),
        ):
            docs.append(enrich_for_es(payload))

    if not docs:
        for m in messages:
            consumer.commit(message=m)
        messages.clear()
        return 0, 0

    BULK_INDEX_BATCH.observe(len(docs))
    with STAGE_SECONDS.labels(stage="es_bulk").time():
        success, errors = index_events(es, docs)
    INDEXER_EVENTS.labels(outcome="indexed").inc(success)
    if errors:
        INDEXER_EVENTS.labels(outcome="error").inc(errors)
    for m in messages:
        consumer.commit(message=m)
    messages.clear()
    return success, errors


def run_indexer() -> None:
    setup_tracing("wikipulse-indexer", "indexer")
    start_metrics_server()
    consumer = make_consumer(
        group_id=f"{settings.kafka_consumer_group}-indexer",
        topics=[settings.kafka_topic_index],
        auto_offset_reset="latest",
    )
    es = get_client()
    log.info("indexer consuming %s -> %s", settings.kafka_topic_index, settings.es_live_write_alias)

    messages: list = []
    last_flush = time.monotonic()
    indexed = 0

    while not should_stop():
        msg = safe_poll(consumer)
        if msg is not None:
            messages.append(msg)
        now = time.monotonic()
        due = now - last_flush >= settings.indexer_flush_interval_s
        if messages and (len(messages) >= settings.indexer_batch_size or due):
            try:
                with span("indexer.flush_batch", batch_size=len(messages)):
                    success, errors = _flush_batch(es, consumer, messages)
                indexed += success
                last_flush = now
                if errors:
                    log.warning("bulk index reported %d errors", errors)
                if indexed % 500 < settings.indexer_batch_size:
                    log.info("indexer total_indexed=%d", indexed)
            except Exception:  # noqa: BLE001
                log.exception("flush failed; offsets not committed, will retry")
                time.sleep(1.0)

    if messages:
        try:
            with span("indexer.flush_batch", batch_size=len(messages)):
                _flush_batch(es, consumer, messages)
        except Exception:  # noqa: BLE001
            log.exception("final flush failed")
    consumer.close()
    log.info("indexer stopped total_indexed=%d", indexed)


def main() -> int:
    configure_logging()
    install_signal_handlers()
    run_indexer()
    return 0


if __name__ == "__main__":
    sys.exit(main())
