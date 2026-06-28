"""SSE producer: Wikimedia EventStreams → ``wiki.raw``.

Connects to the public Wikimedia ``recentchange`` SSE stream, normalizes every
payload to a :class:`WikiEvent`, validates it, and publishes to the ``wiki.raw``
Kafka topic keyed by language. Invalid payloads are routed to ``wiki.deadletter``
so they are never silently dropped.

Resilience:
* exponential backoff reconnect on transport errors;
* partial-event buffering across ``data:`` chunks;
* graceful shutdown on SIGTERM/SIGINT (flush + exit);
* never blocks the event loop on Kafka — librdkafka queues locally.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import sys

import httpx

from app.core.config import settings
from app.events import (
    VALID_EVENT_TYPES,
    ValidationError,
    normalize,
    to_kafka_payload,
    validate,
)
from app.kafka import flush, make_producer, produce
from app.observability import (
    PRODUCER_EVENTS,
    STAGE_SECONDS,
    setup_tracing,
    span,
    start_metrics_server,
)

log = logging.getLogger("wikipulse.producer")


class ProducerStats:
    """Lightweight in-process counters for log lines (and future metrics)."""

    def __init__(self) -> None:
        self.seen = 0
        self.published = 0
        self.invalid = 0
        self.errors = 0

    def snapshot(self) -> dict:
        return {
            "seen": self.seen,
            "published": self.published,
            "invalid": self.invalid,
            "errors": self.errors,
        }


stats = ProducerStats()


def _allowed_types() -> frozenset[str]:
    configured = {t.strip() for t in settings.wikimedia_event_types.split(",") if t.strip()}
    return frozenset(configured) if configured else VALID_EVENT_TYPES


def parse_sse_lines(line: str, buffer: list[str]) -> str | None:
    """Fold one raw SSE line into ``buffer``; return a complete ``data:`` blob.

    SSE events are blank-line terminated. This helper appends ``data:`` payloads
    to ``buffer`` and, on a blank line, joins+returns them. Comment/heartbeat
    lines (``:``) and other fields are ignored. Returns ``None`` until an event
    boundary is reached.
    """
    if line == "":
        if buffer:
            return "\n".join(buffer)
        return None
    if line.startswith(":"):
        return None  # keep-alive comment
    if line.startswith("data:"):
        buffer.append(line[len("data:"):].lstrip())
    return None


async def _publish_event(producer, payload: dict) -> None:
    language = payload.get("language") or "unknown"
    with span("producer.kafka_produce", topic=settings.kafka_topic_raw, language=language):
        produce(producer, settings.kafka_topic_raw, payload, key=language)
    stats.published += 1
    PRODUCER_EVENTS.labels(outcome="published").inc()


async def _handle_raw(producer, raw_text: str, allowed: frozenset[str]) -> None:
    """Parse one SSE ``data`` blob, normalize, validate, and publish."""
    stats.seen += 1
    try:
        with STAGE_SECONDS.labels(stage="sse_parse").time():
            raw = json.loads(raw_text)
            event = normalize(raw)
            validate(event, allowed)
    except (ValidationError, json.JSONDecodeError, ValueError) as exc:
        stats.invalid += 1
        PRODUCER_EVENTS.labels(outcome="deadlettered").inc()
        log.debug("dead-lettering invalid payload: %s", exc)
        produce(
            producer,
            settings.kafka_topic_deadletter,
            {"reason": str(exc), "raw": raw_text[:4000]},
            key="invalid",
        )
        return

    # One span per event — its context is injected into the Kafka message by
    # ``produce`` so demux + consumers continue this trace end-to-end.
    with span(
        "producer.handle_event",
        event_id=event.event_id,
        event_type=event.event_type,
        language=event.language,
    ):
        await _publish_event(producer, to_kafka_payload(event))


async def _stream_once(producer, client: httpx.AsyncClient) -> None:
    """Open one SSE connection and pump events until the socket dies."""
    buffer: list[str] = []
    allowed = _allowed_types()
    async with client.stream(
        "GET",
        settings.wikimedia_stream_url,
        headers={
            "Accept": "text/event-stream",
            "User-Agent": "WikiPulse/0.1 (https://github.com/wikipulse; contact@wikipulse.dev)",
        },
        timeout=httpx.Timeout(
            connect=settings.wikimedia_connect_timeout,
            read=settings.wikimedia_read_timeout or None,
            write=10.0,
            pool=10.0,
        ),
    ) as resp:
        resp.raise_for_status()
        log.info("connected to %s (HTTP %s)", settings.wikimedia_stream_url, resp.status_code)
        async for line in resp.aiter_lines():
            blob = parse_sse_lines(line, buffer)
            if blob is None:
                continue
            buffer = []
            try:
                await _handle_raw(producer, blob, allowed)
            except Exception:  # noqa: BLE001
                stats.errors += 1
                log.exception("failed to handle event")
            # Drain delivery reports without blocking.
            producer.poll(0)


async def run_producer() -> None:
    """Reconnect-with-backoff loop. Intended to run for the process lifetime."""
    setup_tracing("wikipulse-producer", "producer")
    start_metrics_server()
    # The Wikimedia SSE fetch shows up as httpx client spans inside each trace.
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        HTTPXClientInstrumentor().instrument()
    except Exception:  # noqa: BLE001
        log.warning("httpx instrumentation unavailable")

    producer = make_producer(client_id="wikipulse-producer")
    backoff = 2.0
    max_backoff = 60.0
    transport = httpx.AsyncClient(http2=False, follow_redirects=True)

    stop = asyncio.Event()

    def _stop(*_: object) -> None:
        log.info("shutdown signal received")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop = asyncio.get_running_loop()
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _stop())

    try:
        while not stop.is_set():
            try:
                await _stream_once(producer, transport)
                backoff = 2.0  # successful run resets the backoff
            except (httpx.HTTPError, OSError) as exc:
                stats.errors += 1
                log.warning("stream error: %s — reconnecting in %.1fs", exc, backoff)
            except Exception:  # noqa: BLE001
                stats.errors += 1
                log.exception("unexpected producer error")
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            backoff = min(backoff * 2, max_backoff)
    finally:
        await transport.aclose()
        flush(producer, timeout=15.0)
        log.info("producer stopped — stats=%s", stats.snapshot())


def main() -> int:
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run_producer())
    return 0


if __name__ == "__main__":
    sys.exit(main())
