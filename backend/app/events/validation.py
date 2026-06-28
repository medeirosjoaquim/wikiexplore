"""Normalize + validate Wikimedia ``recentchange`` payloads.

The Wikimedia EventStreams ``recentchange`` stream is large and inconsistent
across editions. This module collapses it to :class:`WikiEvent` and exposes
pure, side-effect-free functions that are unit-tested without Kafka or ES.

Reference: https://stream.wikimedia.org/?doc#/streams(recentchange)
"""
from __future__ import annotations

import datetime as _dt
from typing import Any

from app.events.models import WikiEvent

# Event types we care about. ``edit`` and ``new`` are page edits; ``log`` covers
# creations/deletions/blocks. Anything else (categorize, etc.) is dropped.
VALID_EVENT_TYPES = frozenset({"edit", "new", "log"})


class ValidationError(ValueError):
    """Raised when a raw payload cannot be turned into a WikiEvent."""


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


def _parse_timestamp(raw: Any) -> _dt.datetime:
    """Prefer the structured ``meta.dt`` (ISO-8601) over the legacy unix field."""
    if isinstance(raw, str) and raw:
        try:
            return _dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            pass
    if isinstance(raw, int | float) and raw > 0:
        try:
            return _dt.datetime.fromtimestamp(float(raw), tz=_dt.UTC)
        except (OverflowError, OSError, ValueError):
            pass
    return _utc_now()


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _looks_like_ip(user: str) -> bool:
    """Anonymous editors appear as bare IPv4/IPv6 strings with user_id == 0."""
    if not user:
        return False
    return any(ch.isdigit() for ch in user) and ("." in user or ":" in user)


def normalize(raw: dict) -> WikiEvent:
    """Convert a raw Wikimedia payload into a :class:`WikiEvent`.

    Raises :class:`ValidationError` if the payload is missing the fields we
    require to route and store the event.
    """
    if not isinstance(raw, dict):
        raise ValidationError("payload is not an object")

    meta = raw.get("meta") or {}
    event_id = str(meta.get("id") or raw.get("id") or "").strip()
    if not event_id:
        raise ValidationError("missing event id")

    event_type = str(raw.get("type") or "").strip()
    if not event_type:
        raise ValidationError("missing event type")

    # `wiki` is the dbname e.g. "enwiki"; the language prefix is the human form.
    wiki = str(raw.get("wiki") or meta.get("domain") or "").strip()
    lang = str(raw.get("lang") or "").strip()
    if not lang and wiki:
        lang = wiki.replace("wiki", "").replace("wikimedia", "") or "unknown"
    if not wiki:
        wiki = f"{lang or 'unknown'}wiki"
    if not lang:
        lang = "unknown"

    title = str(raw.get("title") or "").strip()
    if not title:
        raise ValidationError("missing page title")

    user = str(raw.get("user") or "").strip()
    if not user:
        raise ValidationError("missing user")

    user_id = _as_int(raw.get("user_id") or raw.get("userid"), 0)
    namespace = _as_int(raw.get("namespace"), 0)

    length = raw.get("length")
    if not isinstance(length, dict):
        length = {}
    old_len = _as_int(length.get("old"))
    new_len = _as_int(length.get("new"))
    delta = new_len - old_len
    bytes_added = delta if delta > 0 else 0
    bytes_removed = -delta if delta < 0 else 0

    revision = raw.get("revision")
    if not isinstance(revision, dict):
        revision = {}
    revision_id = _as_int(revision.get("new") or raw.get("revision_id"))
    old_revision_id = _as_int(revision.get("old") or raw.get("old_revision_id"))

    is_anonymous = bool(user_id == 0 or _looks_like_ip(user))

    return WikiEvent(
        event_id=event_id,
        event_type=event_type,
        timestamp=_parse_timestamp(meta.get("dt") or raw.get("timestamp") or raw.get("dt")),
        language=lang[:16],
        wiki=wiki[:64],
        title=title[:512],
        user=user[:255],
        bot=bool(raw.get("bot")),
        namespace=namespace,
        minor=bool(raw.get("minor")),
        comment=str(raw.get("comment") or "")[:2000],
        page_id=_as_int(raw.get("page_id") or raw.get("pageid")),
        user_id=user_id,
        revision_id=revision_id,
        old_revision_id=old_revision_id,
        bytes_added=bytes_added,
        bytes_removed=bytes_removed,
        is_anonymous=is_anonymous,
        extra={"server_url": str(raw.get("server_url") or ""), "domain": str(meta.get("domain") or "")},
    )


def validate(event: WikiEvent, allowed_types: frozenset[str] | None = None) -> None:
    """Raise :class:`ValidationError` if the event is unusable.

    ``allowed_types`` lets the producer filter by configuration; it defaults to
    :data:`VALID_EVENT_TYPES`.
    """
    allowed = allowed_types if allowed_types is not None else VALID_EVENT_TYPES
    if not event.event_id:
        raise ValidationError("empty event id")
    if event.event_type not in allowed:
        raise ValidationError(f"unsupported event type: {event.event_type}")
    if not event.title:
        raise ValidationError("empty title")
    if not event.user:
        raise ValidationError("empty user")
    if event.timestamp.tzinfo is None:
        raise ValidationError("naive timestamp")


def to_kafka_payload(event: WikiEvent) -> dict:
    """Serialize for Kafka. Timestamps are ISO-8601 strings."""
    return {
        "event_id": event.event_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp.isoformat(),
        "language": event.language,
        "wiki": event.wiki,
        "title": event.title,
        "user": event.user,
        "user_id": event.user_id,
        "bot": event.bot,
        "namespace": event.namespace,
        "minor": event.minor,
        "comment": event.comment,
        "comment_length": len(event.comment),
        "page_id": event.page_id,
        "revision_id": event.revision_id,
        "old_revision_id": event.old_revision_id,
        "bytes_added": event.bytes_added,
        "bytes_removed": event.bytes_removed,
        "is_anonymous": event.is_anonymous,
    }


def to_es_document(event: WikiEvent) -> dict:
    """Map a :class:`WikiEvent` onto the live Elasticsearch index mapping.

    Uses ``@timestamp`` (ES convention) plus a duplicate ``timestamp`` field so
    the index template's explicit mapping resolves cleanly.
    """
    payload = to_kafka_payload(event)
    payload["@timestamp"] = event.timestamp.isoformat()
    payload["ingested_at"] = _utc_now().isoformat()
    return payload


def enrich_for_es(payload: dict) -> dict:
    """Turn a canonical Kafka payload into an Elasticsearch document.

    The Kafka contract (:func:`to_kafka_payload`) carries ``timestamp``; the ES
    mapping + time-based queries/aggregations need the ``@timestamp`` convention
    field. This adds ``@timestamp`` (copied from ``timestamp`` when absent) and
    ``ingested_at``. Used by the indexer so consumers and ES stay decoupled.
    """
    doc = dict(payload)
    ts = doc.get("@timestamp") or doc.get("timestamp")
    if ts:
        doc["@timestamp"] = ts
    doc["ingested_at"] = _utc_now().isoformat()
    return doc
