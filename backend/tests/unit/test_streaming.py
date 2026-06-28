from __future__ import annotations

from app.kafka import deserialize, serialize
from app.search.aliases import hourly_index_name
from app.services.producer import parse_sse_lines


def test_serialize_deserialize_roundtrip():
    payload = {"event_id": "x", "language": "en", "n": 3}
    assert deserialize(serialize(payload)) == payload


def test_deserialize_handles_garbage():
    assert deserialize(b"not json") is None
    assert deserialize(None) is None


def test_parse_sse_assembles_data_lines():
    buffer: list[str] = []
    assert parse_sse_lines("data: {\"a\":", buffer) is None
    assert parse_sse_lines("data: 1}", buffer) is None
    blob = parse_sse_lines("", buffer)  # blank line terminates the event
    assert blob is not None
    assert "{" in blob and "1}" in blob


def test_parse_sse_ignores_comments_and_fields():
    buffer: list[str] = []
    assert parse_sse_lines(": keepalive", buffer) is None
    assert parse_sse_lines("id: 42", buffer) is None
    assert parse_sse_lines("", buffer) is None  # empty buffer -> no event


def test_hourly_index_name_format():
    import datetime as _dt

    name = hourly_index_name(_dt.datetime(2025, 6, 1, 14, 35, tzinfo=_dt.UTC))
    assert name == "wiki-live-events-2025060114"
