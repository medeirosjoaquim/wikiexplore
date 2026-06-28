"""Live broadcast consumer: ``wiki.raw`` → WebSocket hub (in the API process).

The dashboard's live feed is driven by a Kafka consumer that runs inside the
FastAPI process so it can push to the in-memory :class:`~app.api.websocket._Hub`
without cross-process plumbing. It is a *best-effort* tail: at-least-once,
rate-limited, and independent of the durable indexer/analytics consumers.

Tolerates Kafka being temporarily unavailable: it retries forever and simply
streams nothing until the broker returns.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time

from app.api.websocket import hub
from app.core.config import settings
from app.kafka import deserialize, make_consumer

log = logging.getLogger("wikipulse.live")

_consumer_task: asyncio.Task | None = None


class _RateLimiter:
    """Rolling 1-second window limiter — drops bursts above ``max_per_s``."""

    def __init__(self, max_per_s: int) -> None:
        self.max_per_s = max_per_s
        self._window: list[float] = []

    def allow(self) -> bool:
        now = time.monotonic()
        self._window = [t for t in self._window if now - t < 1.0]
        if len(self._window) >= self.max_per_s:
            return False
        self._window.append(now)
        return True


async def _poll_once(consumer) -> list[dict]:
    """Poll for up to a batch of messages (non-blocking-ish via a thread)."""
    try:
        return await asyncio.to_thread(_drain, consumer)
    except Exception as exc:  # noqa: BLE001
        log.warning("live poll failed: %s", exc)
        await asyncio.sleep(2.0)
        return []


def _drain(consumer, max_messages: int = 100) -> list[dict]:
    """Synchronous drain of currently-available messages without blocking long."""
    out: list[dict] = []
    for _ in range(max_messages):
        msg = consumer.poll(0.05)
        if msg is None:
            break
        err = msg.error()
        if err is not None:
            break
        payload = deserialize(msg.value())
        if payload is not None:
            out.append(payload)
    return out


async def _run() -> None:
    limiter = _RateLimiter(settings.live_broadcast_max_rate_per_s)
    consumer = None
    while True:
        if consumer is None:
            try:
                consumer = make_consumer(
                    group_id=settings.live_broadcast_group,
                    topics=[settings.kafka_topic_raw],
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                )
                log.info("live broadcast tailing %s", settings.kafka_topic_raw)
            except Exception as exc:  # noqa: BLE001
                log.warning("live consumer init failed: %s — retrying", exc)
                await asyncio.sleep(5.0)
                continue
        events = await _poll_once(consumer)
        for ev in events:
            if limiter.allow():
                await hub.broadcast({"type": "event", "data": ev})
        if not events:
            await asyncio.sleep(0.25)


async def start() -> None:
    """Start the background tail (idempotent). Called from the app lifespan."""
    global _consumer_task
    if not settings.live_broadcast_enabled:
        log.info("live broadcast disabled by config")
        return
    if _consumer_task is None or _consumer_task.done():
        _consumer_task = asyncio.create_task(_run())


async def stop() -> None:
    global _consumer_task
    if _consumer_task is not None:
        _consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _consumer_task
        _consumer_task = None
