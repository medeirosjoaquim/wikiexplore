"""Explicit Elasticsearch mapping for live Wikipedia events.

Applied as index template `wiki-live-events-template` matching pattern
`wiki-live-events-*`. Tuned for append-only time-series writes with
keyword aggregation and best_compression for cold-friendly storage.
"""
from __future__ import annotations

INDEX_TEMPLATE_NAME = "wiki-live-events-template"
INDEX_PATTERN = "wiki-live-events-*"

TEMPLATE_SETTINGS = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "5s",
    "codec": "best_compression",
}

TEMPLATE_MAPPINGS = {
    "properties": {
        "@timestamp": {"type": "date"},
        "event_id": {"type": "keyword"},
        "event_type": {"type": "keyword"},
        "timestamp": {"type": "date"},
        "language": {"type": "keyword"},
        "wiki": {"type": "keyword"},
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 512}},
        },
        "page_id": {"type": "long"},
        "user": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword", "ignore_above": 255}},
        },
        "user_id": {"type": "long"},
        "bot": {"type": "boolean"},
    }
}

# Embed the comment length / byte delta as numerics so queries/aggregations
# are cheap. Additional free-form fields stay as text.
TEMPLATE_MAPPINGS["properties"].update(
    {
        "comment": {"type": "text"},
        "comment_length": {"type": "integer"},
        "bytes_added": {"type": "long"},
        "bytes_removed": {"type": "long"},
        "minor": {"type": "boolean"},
        "namespace": {"type": "integer"},
        "revision_id": {"type": "long"},
        "old_revision_id": {"type": "long"},
        "suspicious_score": {"type": "float"},
        "reason": {"type": "keyword"},
        "ingested_at": {"type": "date"},
    }
)


def build_template_body() -> dict:
    """Return the full PUT _index_template body."""
    return {
        "index_patterns": [INDEX_PATTERN],
        "template": {
            "settings": TEMPLATE_SETTINGS,
            "mappings": TEMPLATE_MAPPINGS,
        },
        "priority": 100,
        "_meta": {"description": "WikiPulse live events template"},
    }
