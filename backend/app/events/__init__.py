"""Canonical event model + Wikimedia EventStreams normalization.

The producer turns raw Wikimedia ``recentchange`` SSE payloads into a single
canonical :class:`WikiEvent` that every downstream consumer (indexer,
analytics, vandalism) agrees on. Keeping the schema in one place is what lets
the demux fan out to independent topics without each sink re-parsing Wikimedia.
"""
from __future__ import annotations

from .models import WikiEvent
from .validation import (
    VALID_EVENT_TYPES,
    ValidationError,
    enrich_for_es,
    normalize,
    to_es_document,
    to_kafka_payload,
    validate,
)

__all__ = [
    "VALID_EVENT_TYPES",
    "ValidationError",
    "WikiEvent",
    "enrich_for_es",
    "normalize",
    "to_es_document",
    "to_kafka_payload",
    "validate",
]
