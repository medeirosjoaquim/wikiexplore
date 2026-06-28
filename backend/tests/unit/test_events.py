from __future__ import annotations

import pytest

from app.events import (
    VALID_EVENT_TYPES,
    ValidationError,
    enrich_for_es,
    normalize,
    to_es_document,
    to_kafka_payload,
    validate,
)


def _raw(**overrides):
    base = {
        "meta": {"id": "evt-1", "dt": "2025-06-01T10:00:00Z", "domain": "en.wikipedia.org"},
        "id": 1,
        "type": "edit",
        "namespace": 0,
        "title": "Wikipedia",
        "user": "Editor",
        "userid": 42,
        "minor": False,
        "wiki": "enwiki",
        "comment": "fix typo",
        "length": {"old": 1000, "new": 1050},
        "revision": {"old": 10, "new": 11},
        "timestamp": 1748772000,
    }
    base.update(overrides)
    return base


def test_normalize_maps_fields_and_byte_delta():
    ev = normalize(_raw())
    assert ev.event_id == "evt-1"
    assert ev.event_type == "edit"
    assert ev.language == "en"
    assert ev.bytes_added == 50
    assert ev.bytes_removed == 0
    assert ev.revision_id == 11
    assert ev.is_anonymous is False
    assert ev.timestamp.tzinfo is not None


def test_normalize_handles_removal_and_anon():
    ev = normalize(_raw(length={"old": 2000, "new": 100}, user="203.0.113.5", user_id=0))
    assert ev.bytes_removed == 1900
    assert ev.bytes_added == 0
    assert ev.is_anonymous is True


def test_normalize_rejects_missing_id():
    raw = _raw()
    raw["meta"] = {}
    raw["id"] = None
    with pytest.raises(ValidationError):
        normalize(raw)


def test_normalize_rejects_missing_title():
    raw = _raw(title="")
    with pytest.raises(ValidationError):
        normalize(raw)


def test_validate_rejects_disallowed_type():
    ev = normalize(_raw(type="categorize"))
    with pytest.raises(ValidationError):
        validate(ev)


def test_validate_all_list_filters_unknown_type():
    ev = normalize(_raw(type="categorize"))
    # When the allow-list is restrictive, validation fails.
    with pytest.raises(ValidationError):
        validate(ev, allowed_types=frozenset({"edit", "new"}))


def test_to_kafka_payload_roundtrips_scalars():
    ev = normalize(_raw())
    payload = to_kafka_payload(ev)
    assert payload["event_id"] == "evt-1"
    assert payload["comment_length"] == len("fix typo")
    assert payload["bytes_added"] == 50
    assert isinstance(payload["timestamp"], str)


def test_to_es_document_has_timestamp_and_ingested_at():
    doc = to_es_document(normalize(_raw()))
    assert "@timestamp" in doc
    assert "ingested_at" in doc
    assert "timestamp" in doc


def test_valid_event_types_contains_core():
    assert {"edit", "new", "log"} <= VALID_EVENT_TYPES


def test_enrich_for_es_adds_at_timestamp():
    payload = {"event_id": "x", "timestamp": "2026-06-28T04:05:00+00:00", "language": "en"}
    doc = enrich_for_es(payload)
    assert doc["@timestamp"] == "2026-06-28T04:05:00+00:00"
    assert "ingested_at" in doc
    # original payload is not mutated
    assert "@timestamp" not in payload
