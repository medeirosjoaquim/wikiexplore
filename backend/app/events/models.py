"""Canonical Wikipedia edit event used across the pipeline."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime


@dataclass(frozen=True, slots=True)
class WikiEvent:
    """A single normalized Wikipedia edit event.

    This is the ONE shape that flows through Kafka. Producers build it,
    consumers depend on it. Field names mirror the Elasticsearch live mapping
    so :func:`app.events.to_es_document` is a near-identity transform.
    """

    event_id: str
    event_type: str
    timestamp: datetime
    language: str
    wiki: str
    title: str
    user: str
    bot: bool
    namespace: int = 0
    minor: bool = False
    comment: str = ""
    page_id: int = 0
    user_id: int = 0
    revision_id: int = 0
    old_revision_id: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    is_anonymous: bool = False
    # Free-form metadata that survives into ES but not into PG aggregates.
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return asdict(self)
