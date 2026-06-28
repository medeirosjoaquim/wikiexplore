"""Idempotent Elasticsearch initialization.

Steps:
  1. create or update index template `wiki-live-events-template`
  2. create the current hourly index if missing
  3. ensure write alias points at exactly one active hourly index
  4. reconcile read alias over all retained hourly indices
  5. remove stale/broken aliases safely

Safe to run repeatedly.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import sys
import time

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from app.core.config import settings  # noqa: E402
from app.search.index_templates import (  # noqa: E402
    INDEX_PATTERN,
    INDEX_TEMPLATE_NAME,
    build_template_body,
)
from app.search.aliases import (  # noqa: E402
    get_client,
    hourly_index_name,
    list_live_indices,
)


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _hour_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def _retained_hours() -> list[_dt.datetime]:
    now = _hour_floor(_utc_now())
    return [now - _dt.timedelta(hours=h) for h in range(settings.live_retention_hours)]


def main() -> int:
    parser = argparse.ArgumentParser(description="Initialize Elasticsearch")
    parser.add_argument("--url", default=settings.elasticsearch_url)
    parser.add_argument("--timeout", default=120, type=int)
    args = parser.parse_args()

    # 1. Wait for cluster health.
    deadline = time.time() + args.timeout
    es = get_client()
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            health = es.cluster.health(wait_for_status="yellow", timeout="5s")
            status = health.get("status")
            if status in ("green", "yellow"):
                print(f"[init_es] cluster healthy (status={status})")
                break
        except Exception as exc:  # noqa: BLE001
            print(f"[init_es] waiting attempt {attempt}: {exc}", file=sys.stderr)
        time.sleep(3)
    else:
        print("[init_es] timed out waiting for cluster", file=sys.stderr)
        return 1

    # 2. Create / update index template.
    template_body = build_template_body()
    try:
        es.indices.put_index_template(name=INDEX_TEMPLATE_NAME, **template_body)
        print(f"[init_es] upserted index template '{INDEX_TEMPLATE_NAME}' "
              f"(pattern={INDEX_PATTERN})")
    except Exception as exc:  # noqa: BLE001
        print(f"[init_es] failed to put index template: {exc}", file=sys.stderr)
        return 1

    # 3. Ensure current hourly index exists.
    current_index = hourly_index_name()
    try:
        if not es.indices.exists(index=current_index):
            es.indices.create(index=current_index)
            print(f"[init_es] created live index '{current_index}'")
        else:
            print(f"[init_es] live index exists '{current_index}'")
    except Exception as exc:  # noqa: BLE001
        print(f"[init_es] failed to ensure index '{current_index}': {exc}", file=sys.stderr)
        return 1

    # 4. Reconcile write alias -> exactly the current hourly index.
    try:
        existing = list_live_indices(es)
        write_actions = []
        # Drop any index currently bound to the write alias.
        for name in existing:
            try:
                if es.indices.exists_alias(index=name, name=settings.es_live_write_alias):
                    write_actions.append(
                        {"remove": {"index": name, "alias": settings.es_live_write_alias}}
                    )
            except Exception:
                continue
        write_actions.append(
            {"add": {"index": current_index, "alias": settings.es_live_write_alias}}
        )
        es.indices.update_aliases(actions=write_actions)
        print(f"[init_es] write alias '{settings.es_live_write_alias}' "
              f"-> '{current_index}'")
    except Exception as exc:  # noqa: BLE001
        print(f"[init_es] failed to set write alias: {exc}", file=sys.stderr)
        return 1

    # 5. Reconcile read alias over retained indices.
    try:
        retained = [hourly_index_name(h) for h in _retained_hours()]
        # Only include indices that actually exist.
        retained = [name for name in retained if es.indices.exists(index=name)]
        read_actions = []
        # First strip the read alias from everything currently bound.
        for name in existing:
            try:
                if es.indices.exists_alias(index=name, name=settings.es_live_read_alias):
                    read_actions.append(
                        {"remove": {"index": name, "alias": settings.es_live_read_alias}}
                    )
            except Exception:
                continue
        for name in retained:
            read_actions.append(
                {"add": {"index": name, "alias": settings.es_live_read_alias}}
            )
        if read_actions:
            es.indices.update_aliases(actions=read_actions)
        print(
            f"[init_es] read alias '{settings.es_live_read_alias}' "
            f"-> {len(retained)} index(/ices)"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[init_es] failed to reconcile read alias: {exc}", file=sys.stderr)
        return 1

    print("[init_es] elasticsearch ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
