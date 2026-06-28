"""Live search endpoints backed by Elasticsearch (within the retention window)."""
from __future__ import annotations

import datetime as _dt

from fastapi import APIRouter, Query

from app.core.config import settings

router = APIRouter()


def _client():
    from elasticsearch import Elasticsearch

    return Elasticsearch(settings.elasticsearch_url, request_timeout=10)


def _build_query(
    q: str,
    language: str | None,
    bot: bool | None,
    anonymous: bool | None,
    namespace: int | None,
) -> dict:
    must: list[dict] = []
    filter_clauses: list[dict] = []
    if q:
        must.append({"multi_match": {"query": q, "fields": ["title", "user", "comment"]}})
    if language:
        filter_clauses.append({"term": {"language": language}})
    if bot is not None:
        filter_clauses.append({"term": {"bot": bot}})
    if anonymous is not None:
        filter_clauses.append({"term": {"is_anonymous": anonymous}})
    if namespace is not None:
        filter_clauses.append({"term": {"namespace": namespace}})
    bool_q: dict = {}
    if must:
        bool_q["must"] = must
    if filter_clauses:
        bool_q["filter"] = filter_clauses
    return {"bool": bool_q} if bool_q else {"match_all": {}}


@router.get("/live/search")
def live_search(
    q: str = Query("", description="title or user substring"),
    language: str | None = None,
    bot: bool | None = None,
    anonymous: bool | None = None,
    namespace: int | None = None,
    size: int = Query(50, ge=1, le=500),
) -> dict:
    es = _client()
    body = {
        "size": size,
        "sort": [{"@timestamp": {"order": "desc"}}],
        "query": _build_query(q, language, bot, anonymous, namespace),
    }
    try:
        started = _dt.datetime.now(_dt.UTC)
        resp = es.search(index=settings.es_live_read_alias, body=body)
        took_ms = int((_dt.datetime.now(_dt.UTC) - started).total_seconds() * 1000)
    except Exception as exc:  # noqa: BLE001
        return {"total": 0, "hits": [], "took_ms": None, "error": str(exc)}
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    total = resp.get("hits", {}).get("total", {})
    if isinstance(total, dict):
        total = total.get("value", 0)
    return {"total": total, "hits": hits, "took_ms": took_ms}


@router.get("/live/namespaces")
def live_namespaces(size: int = Query(20, ge=1, le=50)) -> list[dict]:
    """Namespace breakdown of live events (edits by namespace)."""
    es = _client()
    try:
        resp = es.search(
            index=settings.es_live_read_alias,
            body={
                "size": 0,
                "aggs": {"ns": {"terms": {"field": "namespace", "size": size}}},
            },
        )
    except Exception:  # noqa: BLE001
        return []
    return [{"namespace": int(b["key"]), "edits": int(b["doc_count"])} for b in resp["aggregations"]["ns"]["buckets"]]


@router.get("/live/rate")
def live_rate(minutes: int = Query(15, ge=1, le=120)) -> dict:
    """Edits-per-minute histogram over the last ``minutes`` (live window only)."""
    es = _client()
    now = _dt.datetime.now(_dt.UTC)
    since = now - _dt.timedelta(minutes=minutes)
    try:
        resp = es.search(
            index=settings.es_live_read_alias,
            body={
                "size": 0,
                "query": {"range": {"@timestamp": {"gte": since.isoformat()}}},
                "aggs": {
                    "per_minute": {
                        "date_histogram": {
                            "field": "@timestamp",
                            "fixed_interval": "1m",
                            "min_doc_count": 0,
                        }
                    }
                },
            },
        )
    except Exception as exc:  # noqa: BLE001
        return {"points": [], "error": str(exc)}
    points = [
        {"ts": b["key_as_string"], "edits": int(b["doc_count"])}
        for b in resp["aggregations"]["per_minute"]["buckets"]
    ]
    total = sum(p["edits"] for p in points)
    edits_per_minute = total / minutes if minutes else 0
    return {"points": points, "edits_per_minute": round(edits_per_minute, 1), "total": total}
