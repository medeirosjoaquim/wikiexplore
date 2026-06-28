"""Elasticsearch live-index alias management.

Aliases:
  wiki-live-events-write  -> exactly one active hourly index (producer writes here)
  wiki-live-events-read   -> all retained hourly indices (search reads here)

Indices are named ``wiki-live-events-YYYYMMDDHH`` (UTC).
"""
from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

from elasticsearch import Elasticsearch

from app.core.config import settings
from app.search.index_templates import INDEX_TEMPLATE_NAME

log = logging.getLogger("wikipulse.search.aliases")


def _now_hourly(dt: _dt.datetime | None = None) -> _dt.datetime:
    dt = dt or _dt.datetime.now(_dt.UTC)
    return dt.replace(minute=0, second=0, microsecond=0)


def hourly_index_name(dt: _dt.datetime | None = None) -> str:
    """Return the concrete index name for the hour containing ``dt``."""
    hour = _now_hourly(dt)
    return f"{settings.es_live_index_prefix}-{hour.strftime('%Y%m%d%H')}"

def get_client() -> Elasticsearch:
    return Elasticsearch(settings.elasticsearch_url, request_timeout=30)


def template_exists(es: Elasticsearch) -> bool:
    return bool(es.indices.exists_index_template(name=INDEX_TEMPLATE_NAME))


def _keys(result: Any) -> list[str]:
    """Return the index-name keys from an ES response (dict OR ObjectApiResponse).

    elasticsearch-py 8.x returns ``ObjectApiResponse`` objects, which are NOT
    ``dict`` instances despite behaving like one — so ``isinstance(result, dict)``
    silently returns False and loses every key. This helper is the robust form.
    """
    keys = getattr(result, "keys", None)
    if callable(keys):
        return list(keys())
    if isinstance(result, dict):
        return list(result.keys())
    return []


def list_live_indices(es: Elasticsearch) -> list[str]:
    pattern = f"{settings.es_live_index_prefix}-*"
    try:
        result = es.indices.get(index=pattern)
    except Exception:
        return []
    return sorted(_keys(result))


def read_alias_actions(desired: list[str]) -> list[dict[str, Any]]:
    """Build actions so the read alias points at exactly ``desired`` indices."""
    actions: list[dict[str, Any]] = []
    current = _alias_indices(es=None, alias=settings.es_live_read_alias)
    for name in set(current) - set(desired):
        actions.append({"remove": {"index": name, "alias": settings.es_live_read_alias}})
    for name in set(desired) - set(current):
        actions.append({"add": {"index": name, "alias": settings.es_live_read_alias}})
    return actions


def _alias_indices(es: Elasticsearch | None, alias: str) -> set[str]:
    es = es or get_client()
    try:
        result = es.indices.get_alias(name=alias)
    except Exception:
        return set()
    return set(_keys(result))


def ensure_current_hour_index(es: Elasticsearch, now: _dt.datetime | None = None) -> str:
    """Create the current hourly index if missing and pin the write alias to it.

    The write alias is collapsed to exactly the current hour and that index is
    designated ``is_write_index=true`` so ES accepts writes unambiguously even
    if stale indices were left bound by a previous run. Idempotent.
    """
    hour = _now_hourly(now)
    name = hourly_index_name(hour)

    if not es.indices.exists(index=name):
        es.indices.create(index=name)
        log.info("created live index %s", name)

    # Write alias must point at exactly one index: the current hour.
    current_write = _alias_indices(es, settings.es_live_write_alias)
    if current_write != {name}:
        actions: list[dict[str, Any]] = [
            {"remove": {"index": idx, "alias": settings.es_live_write_alias}}
            for idx in current_write
            if idx != name
        ]
        actions.append(
            {"add": {"index": name, "alias": settings.es_live_write_alias, "is_write_index": True}}
        )
        es.indices.update_aliases(actions=actions)

    # Read alias covers this index too (cleanup removes expired ones later).
    read_indices = _alias_indices(es, settings.es_live_read_alias)
    if name not in read_indices:
        es.indices.update_aliases(
            actions=[{"add": {"index": name, "alias": settings.es_live_read_alias}}]
        )
    return name
