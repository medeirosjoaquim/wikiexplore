"""ES indexer consumer: ``wiki.index`` → Elasticsearch live index.

Batches messages, ensures the current hourly index exists, bulk-indexes to the
``wiki-live-events-write`` alias, and only then commits offsets — so a crash
before commit means events are re-delivered and re-indexed idempotently
(keyed on ``event_id``).
"""
from __future__ import annotations

import logging
import sys
import time

from app.core.config import settings
from app.kafka import deserialize, make_consumer, safe_poll
from app.search.aliases import get_client
from app.search.writer import index_events
from app.services.runtime import configure_logging, install_signal_handlers, should_stop

log = logging.getLogger("wikipulse.indexer")


def _flush_batch(es, consumer, batch: list, messages: list) -> tuple[int, int]:
    from app.events import enrich_for_es

    docs = [enrich_for_es(d) for d in (deserialize(m.value()) for m in messages) if d]
    success, errors = index_events(es, docs)
    # Commit offsets only after a successful bulk write.
    for m in messages:
        consumer.commit(message=m)
    batch.clear()
    messages.clear()
    return success, errors


def run_indexer() -> None:
    consumer = make_consumer(
        group_id=f"{settings.kafka_consumer_group}-indexer",
        topics=[settings.kafka_topic_index],
        auto_offset_reset="latest",
    )
    es = get_client()
    log.info("indexer consuming %s -> %s", settings.kafka_topic_index, settings.es_live_write_alias)

    batch: list = []
    messages: list = []
    last_flush = time.monotonic()
    indexed = 0

    while not should_stop():
        msg = safe_poll(consumer)
        if msg is not None:
            messages.append(msg)
            batch.append(msg.value())
        now = time.monotonic()
        due = now - last_flush >= settings.indexer_flush_interval_s
        if messages and (len(messages) >= settings.indexer_batch_size or due):
            try:
                success, errors = _flush_batch(es, consumer, batch, messages)
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
            _flush_batch(es, consumer, batch, messages)
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
