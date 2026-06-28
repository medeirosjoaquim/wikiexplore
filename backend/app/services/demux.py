"""Demux consumer: ``wiki.raw`` → ``{wiki.index, wiki.analytics, wiki.vandalism}``.

The producer writes a single canonical stream (``wiki.raw``). This service fans
it out into one topic per downstream concern so each sink can be restarted,
replayed, and scaled independently — the core of ADR-001 (Kafka is the single
source of event distribution) and ADR-004 (loose coupling).

Exactly-once across topics is intentionally NOT attempted: each downstream
consumer is idempotent on ``event_id``, so at-least-once delivery here is safe.

Each message continues the producer's distributed trace via the W3C context
carried in the Kafka headers, and the fan-out publishes inherit that context.
"""
from __future__ import annotations

import logging
import sys

from app.core.config import settings
from app.kafka import deserialize, flush, make_consumer, make_producer, produce, safe_poll
from app.observability import (
    DEMUX_FORWARDED,
    KAFKA_POLL_BATCH,
    extract_context,
    setup_tracing,
    span,
    start_metrics_server,
)
from app.services.runtime import configure_logging, install_signal_handlers, should_stop

log = logging.getLogger("wikipulse.demux")

FANOUT_TOPICS = (
    settings.kafka_topic_index,
    settings.kafka_topic_analytics,
    settings.kafka_topic_vandalism,
)


def fanout(producer, payload: dict, key: str | None) -> int:
    """Republish ``payload`` to every fan-out topic. Returns topics written.

    Runs inside the caller's span, so each re-published message carries the
    propagated trace context onward to the downstream consumers.
    """
    written = 0
    for topic in FANOUT_TOPICS:
        produce(producer, topic, payload, key=key)
        DEMUX_FORWARDED.labels(topic=topic).inc()
        written += 1
    return written


def run_demux() -> None:
    setup_tracing("wikipulse-demux", "demux")
    start_metrics_server()
    consumer = make_consumer(
        group_id=f"{settings.kafka_consumer_group}-demux",
        topics=[settings.kafka_topic_raw],
        auto_offset_reset="earliest",
    )
    producer = make_producer(client_id="wikipulse-demux")
    log.info("demux consuming %s -> %s", settings.kafka_topic_raw, list(FANOUT_TOPICS))

    seen = forwarded = 0
    batch = 0
    while not should_stop():
        msg = safe_poll(consumer)
        if msg is None:
            producer.poll(0)
            if batch:
                KAFKA_POLL_BATCH.labels(consumer="demux").observe(batch)
                batch = 0
            continue
        batch += 1
        payload = deserialize(msg.value())
        if payload is None:
            consumer.commit(message=msg)
            continue
        seen += 1
        key = (msg.key() or b"").decode("utf-8", "ignore") or payload.get("language")
        ctx = extract_context(msg.headers())
        try:
            with span(
                "demux.forward",
                context=ctx,
                event_id=payload.get("event_id"),
                topic=settings.kafka_topic_raw,
                partition=msg.partition(),
                offset=msg.offset(),
            ):
                forwarded += fanout(producer, payload, key=key)
        except BufferError:
            # Queue full — back off and retry this message without committing.
            producer.poll(0.5)
            continue
        consumer.commit(message=msg)
        if seen % 1000 == 0:
            log.info("demux seen=%d forwarded=%d", seen, forwarded)
            producer.poll(0)

    consumer.close()
    flush(producer, timeout=15.0)
    log.info("demux stopped seen=%d forwarded=%d", seen, forwarded)


def main() -> int:
    configure_logging()
    install_signal_handlers()
    run_demux()
    return 0


if __name__ == "__main__":
    sys.exit(main())
