from __future__ import annotations

import importlib


def test_index_template_shape():
    mod = importlib.import_module("app.search.index_templates")
    body = mod.build_template_body()
    assert body["index_patterns"] == ["wiki-live-events-*"]
    settings = body["template"]["settings"]
    assert settings["number_of_shards"] == 1
    assert settings["number_of_replicas"] == 0
    assert settings["refresh_interval"] == "5s"
    assert settings["codec"] == "best_compression"
    props = body["template"]["mappings"]["properties"]
    for required in ("@timestamp", "language", "title", "user", "bot", "timestamp"):
        assert required in props, required


def test_kafka_topic_partition_plan():
    from app.core.config import settings

    topics = settings.kafka_topics
    assert topics["wiki.raw"]["partitions"] == 3
    assert topics["wiki.index"]["partitions"] == 3
    assert topics["wiki.analytics"]["partitions"] == 3
    assert topics["wiki.vandalism"]["partitions"] == 2
    assert topics["wiki.deadletter"]["partitions"] == 1
