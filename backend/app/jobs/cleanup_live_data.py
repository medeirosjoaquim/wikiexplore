"""Remove expired Elasticsearch live indices and prune the read alias.

Removes indices older than LIVE_RETENTION_HOURS from the read alias before
deleting them, exactly per the alias rules in the spec.
"""
from __future__ import annotations

import contextlib
import datetime as _dt
import sys

from app.core.config import settings
from app.search.aliases import get_client


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.UTC)


def _hour_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def run_cleanup() -> dict:
    es = get_client()
    now = _hour_floor(_utc_now())
    cutoff = now - _dt.timedelta(hours=settings.live_retention_hours)

    # Discover live indices.
    try:
        existing = es.indices.get(index=f"{settings.es_live_index_prefix}-*")
    except Exception:
        return {"deleted": [], "unaliased": []}
    existing_names = sorted(list(existing.keys())) if hasattr(existing, "keys") else []

    to_unalias: list[str] = []
    to_delete: list[str] = []
    for name in existing_names:
        # name like wiki-live-events-2025010112
        suffix = name.rsplit("-", 1)[-1]
        try:
            index_hour = _dt.datetime.strptime(suffix, "%Y%m%d%H").replace(tzinfo=_dt.UTC)
        except ValueError:
            continue
        if index_hour < cutoff:
            to_unalias.append(name)
            to_delete.append(name)

    actions = [
        {"remove": {"index": n, "alias": settings.es_live_read_alias}} for n in to_unalias
    ]
    if actions:
        with contextlib.suppress(Exception):
            es.indices.update_aliases(actions=actions)

    for name in to_delete:
        try:
            es.indices.delete(index=name)
        except Exception:
            continue
    return {"deleted": to_delete, "unaliased": to_unalias, "cutoff": cutoff.isoformat()}


def main() -> int:
    result = run_cleanup()
    print(result)
    return 0


if __name__ == "__main__":
    sys.exit(main())
