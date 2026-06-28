"""Thin wrapper over ``confluent-kafka`` for produce/consume.

All Kafka I/O in WikiPulse goes through this module so the rest of the
codebase depends on our helpers, not on the vendor client directly. That keeps
serialization, error handling, and offset commits in one place.

Why ``confluent-kafka``: it is the maintained librdkafka binding. The older
``kafka-python`` is unmaintained and breaks on Python 3.12.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterable
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, KafkaError, KafkaException, Message, Producer

from app.core.config import settings

log = logging.getLogger("wikipulse.kafka")


# ─── serialization ──────────────────────────────────────────────────────────

def serialize(value: dict) -> bytes:
    """JSON-encode with a stable separator set (smaller payloads, no spaces)."""
    return json.dumps(value, separators=(",", ":"), default=str).encode("utf-8")


def deserialize(raw: bytes | None) -> dict | None:
    if not raw:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        log.warning("dropping undecodable message: %s", exc)
        return None


# ─── producer ───────────────────────────────────────────────────────────────

def _delivery_report(err: KafkaError | None, msg: Message) -> None:
    """librdkafka delivery callback — invoked from ``Producer.poll``."""
    if err is not None:
        log.error("delivery failed for %s: %s", msg.topic(), err.str())


def make_producer(**overrides: Any) -> Producer:
    conf: dict[str, Any] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": overrides.pop("client_id", "wikipulse-producer"),
        "compression.codec": "lz4",
        "linger.ms": 50,
        "batch.size": 65536,
        "enable.idempotence": settings.producer_idempotent,
        "message.timeout.ms": 30000,
        "queue.buffering.max.messages": 200000,
    }
    conf.update(overrides)
    return Producer(conf)


def produce(
    producer: Producer,
    topic: str,
    value: dict,
    key: str | None = None,
    headers: dict[str, str] | None = None,
    on_delivery: Callable[[KafkaError | None, Message], None] | None = None,
) -> None:
    """Produce one JSON message and drain the delivery queue once.

    Raises :class:`BufferError` if the internal queue is full; callers decide
    whether to retry, block, or dead-letter. ``key`` enables partition affinity
    (we key on language so a single language's edits stay ordered).
    """
    producer.produce(
        topic=topic,
        value=serialize(value),
        key=key.encode("utf-8") if key else None,
        headers=[(k, v.encode("utf-8")) for k, v in (headers or {}).items()] or None,
        on_delivery=on_delivery or _delivery_report,
    )
    producer.poll(0)


def flush(producer: Producer, timeout: float = 10.0) -> None:
    remaining = producer.flush(timeout)
    if remaining > 0:
        log.warning("%d messages still in-flight after flush(%ss)", remaining, timeout)


# ─── consumer ───────────────────────────────────────────────────────────────

def make_consumer(
    group_id: str,
    topics: Iterable[str],
    *,
    auto_offset_reset: str | None = None,
    enable_auto_commit: bool | None = None,
    **overrides: Any,
) -> Consumer:
    conf: dict[str, Any] = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": settings.consumer_enable_auto_commit.lower() == "true",
        "auto.offset.reset": auto_offset_reset or settings.consumer_auto_offset_reset,
        "session.timeout.ms": 45000,
        "max.poll.interval.ms": 300000,
        "fetch.min.bytes": 1,
    }
    if enable_auto_commit is not None:
        conf["enable.auto.commit"] = enable_auto_commit
    conf.update(overrides)
    consumer = Consumer(conf)
    consumer.subscribe(list(topics))
    return consumer


def safe_poll(consumer: Consumer, timeout: float | None = None) -> Message | None:
    """Poll once, returning ``None`` for the common non-error cases.

    Raises :class:`KafkaException` only on unrecoverable errors so the caller's
    loop can decide its restart policy.
    """
    msg = consumer.poll(timeout if timeout is not None else settings.consumer_poll_timeout)
    if msg is None:
        return None
    err = msg.error()
    if err is None:
        return msg
    # End-of-partition is normal, not an error.
    if err.code() == KafkaError._PARTITION_EOF:  # noqa: SLF001
        return None
    raise KafkaException(err)


__all__ = [
    "OFFSET_END",
    "Consumer",
    "Producer",
    "Message",
    "deserialize",
    "flush",
    "make_consumer",
    "make_producer",
    "produce",
    "safe_poll",
    "serialize",
]
