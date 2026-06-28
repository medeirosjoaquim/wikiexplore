from __future__ import annotations

from app.services import demux


def test_fanout_writes_every_topic_once(monkeypatch):
    calls: list[tuple[str, dict, str | None]] = []

    def fake_produce(producer, topic, value, key=None, **_):
        calls.append((topic, value, key))

    monkeypatch.setattr(demux, "produce", fake_produce)

    written = demux.fanout(object(), {"event_id": "1", "language": "en"}, key="en")

    assert written == 3
    topics = [c[0] for c in calls]
    assert demux.settings.kafka_topic_index in topics
    assert demux.settings.kafka_topic_analytics in topics
    assert demux.settings.kafka_topic_vandalism in topics
    assert all(c[1] == {"event_id": "1", "language": "en"} for c in calls)
    assert all(c[2] == "en" for c in calls)


def test_demux_topics_cover_all_downstreams():
    # The fan-out topics must match the topology in the README/SRS.
    topics = set(demux.FANOUT_TOPICS)
    s = demux.settings
    assert topics == {s.kafka_topic_index, s.kafka_topic_analytics, s.kafka_topic_vandalism}
