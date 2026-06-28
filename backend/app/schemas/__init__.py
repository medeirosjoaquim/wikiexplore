"""Pydantic response models — typed API contracts for the frontend."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class Overview(BaseModel):
    hour: datetime | None = None
    total_edits: int = 0
    minor_edits: int = 0
    total_bytes_added: int = 0
    total_bytes_removed: int = 0
    active_users: int = 0
    active_pages: int = 0
    distinct_languages: int = 0
    source: str | None = None


class LanguageStat(BaseModel):
    language: str
    edits: int
    bytes_added: int = 0


class PageStat(BaseModel):
    language: str
    page_title: str
    edits: int


class UserStat(BaseModel):
    username: str
    is_bot: bool
    edits: int


class SuspiciousEdit(BaseModel):
    id: int
    hour: datetime | None = None
    detected_at: datetime | None = None
    language: str
    page_title: str
    username: str | None = None
    score: float
    reason: str
    source: str


class TimeseriesPoint(BaseModel):
    hour: datetime
    total_edits: int
    minor_edits: int = 0
    active_users: int = 0


class LiveHit(BaseModel):
    event_id: str | None = None
    timestamp: str | None = None
    language: str | None = None
    title: str | None = None
    user: str | None = None
    bot: bool | None = None
    minor: bool | None = None
    comment: str | None = None
    bytes_added: int | None = None
    bytes_removed: int | None = None


class LiveSearchResult(BaseModel):
    total: int = 0
    hits: list[dict] = Field(default_factory=list)
    took_ms: int | None = None
    error: str | None = None


class SystemStatus(BaseModel):
    consolidation: dict
    retention: dict
    storage: dict
    live_retention_hours: int
    consolidation_window_hours: int
