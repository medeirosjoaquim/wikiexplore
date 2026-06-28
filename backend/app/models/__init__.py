"""ORM models. The schema is created by Alembic migrations; these models
describe the same tables for the application layer.

Tables
------
consolidated_windows      3-hour rollup job tracking
hourly_wiki_stats         aggregate edit counts per hour (global)
hourly_language_stats     aggregate per language per hour
hourly_page_stats         aggregate per page per hour
hourly_user_stats         aggregate per user per hour
suspicious_edit_summary   vandalism candidates
app_settings              key/value configuration
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database.base import Base


class ConsolidatedWindow(Base):
    """Tracks the state of each 3-hour consolidation window."""

    __tablename__ = "consolidated_windows"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="pending")
    rows_consolidated: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint("window_start", "window_end", name="uq_consolidated_windows_window"),
        CheckConstraint(
            "status in ('pending','running','completed','failed')",
            name="ck_consolidated_windows_status",
        ),
        Index("ix_consolidated_windows_status", "status"),
        Index("ix_consolidated_windows_window_start", "window_start"),
    )


class HourlyWikiStats(Base):
    """Global aggregate edit statistics per hour."""

    __tablename__ = "hourly_wiki_stats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    total_edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    minor_edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_bytes_added: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    total_bytes_removed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    active_users: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_pages: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    distinct_languages: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="live")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (UniqueConstraint("hour", "source", name="uq_hourly_wiki_stats_hour_source"),)


class HourlyLanguageStats(Base):
    """Aggregate edit statistics per language per hour."""

    __tablename__ = "hourly_language_stats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    minor_edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_added: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_removed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    active_users: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="live")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "hour", "language", "source", name="uq_hourly_language_stats_hour_lang_source"
        ),
        Index("ix_hourly_language_stats_hour", "hour"),
        Index("ix_hourly_language_stats_language", "language"),
        Index("ix_hourly_language_stats_edits", "edits"),
    )


class HourlyPageStats(Base):
    """Aggregate edit statistics per page per hour."""

    __tablename__ = "hourly_page_stats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    page_title: Mapped[str] = mapped_column(String(512), nullable=False)
    page_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_added: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_removed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    distinct_users: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="live")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "hour", "language", "page_title", "source",
            name="uq_hourly_page_stats_hour_lang_page_source",
        ),
        Index("ix_hourly_page_stats_hour", "hour"),
        Index("ix_hourly_page_stats_language", "language"),
        Index("ix_hourly_page_stats_edits", "edits"),
        Index("ix_hourly_page_stats_page_id", "page_id"),
    )


class HourlyUserStats(Base):
    """Aggregate edit statistics per user per hour."""

    __tablename__ = "hourly_user_stats"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    username: Mapped[str] = mapped_column(String(255), nullable=False)
    edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    minor_edits: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_added: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    bytes_removed: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    is_bot: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="live")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint(
            "hour", "username", "source", name="uq_hourly_user_stats_hour_user_source"
        ),
        Index("ix_hourly_user_stats_hour", "hour"),
        Index("ix_hourly_user_stats_edits", "edits"),
        Index("ix_hourly_user_stats_is_bot", "is_bot"),
    )


class SuspiciousEditSummary(Base):
    """Vandalism / suspicious edit candidates surfaced by detection."""

    __tablename__ = "suspicious_edit_summary"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    hour: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    language: Mapped[str] = mapped_column(String(16), nullable=False)
    page_title: Mapped[str] = mapped_column(String(512), nullable=False)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    reason: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="live")

    __table_args__ = (
        Index("ix_suspicious_edit_summary_hour", "hour"),
        Index("ix_suspicious_edit_summary_detected_at", "detected_at"),
        Index("ix_suspicious_edit_summary_score", "score"),
        Index("ix_suspicious_edit_summary_language", "language"),
    )


class AppSettings(Base):
    """Simple key/value store for runtime configuration."""

    __tablename__ = "app_settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


__all__ = [
    "AppSettings",
    "ConsolidatedWindow",
    "HourlyLanguageStats",
    "HourlyPageStats",
    "HourlyUserStats",
    "HourlyWikiStats",
    "SuspiciousEditSummary",
]
