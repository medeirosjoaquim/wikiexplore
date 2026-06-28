"""Bulk Elasticsearch indexing for the live events alias."""
from __future__ import annotations

import logging
from collections.abc import Iterable

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from app.core.config import settings
from app.search.aliases import ensure_current_hour_index

log = logging.getLogger("wikipulse.search.writer")


def index_events(es: Elasticsearch, documents: Iterable[dict]) -> tuple[int, int]:
    """Bulk-index documents into the live write alias.

    Each document is keyed by ``event_id`` so re-delivery (at-least-once) is a
    no-op overwrite, not a duplicate. Returns ``(success, errors)``.
    """
    docs = list(documents)
    if not docs:
        return 0, 0

    ensure_current_hour_index(es)

    def _actions() -> Iterable[dict]:
        for doc in docs:
            yield {
                "_op_type": "index",
                "_index": settings.es_live_write_alias,
                "_id": doc.get("event_id"),
                "_source": doc,
            }

    success, errors = bulk(
        es,
        _actions(),
        raise_on_error=False,
        raise_on_exception=False,
        request_timeout=30,
        stats_only=False,
    )
    return success, len(errors) if isinstance(errors, list) else int(errors)
