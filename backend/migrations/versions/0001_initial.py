"""create initial analytics tables

Revision ID: 0001_initial
Revises:
Create Date: 2025-01-01 00:00:00
"""
from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── consolidated_windows ──────────────────────────────────
    op.create_table(
        "consolidated_windows",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="pending"),
        sa.Column("rows_consolidated", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status in ('pending','running','completed','failed')",
            name="ck_consolidated_windows_status",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "window_start", "window_end", name="uq_consolidated_windows_window"
        ),
    )
    op.create_index(
        "ix_consolidated_windows_status", "consolidated_windows", ["status"]
    )
    op.create_index(
        "ix_consolidated_windows_window_start",
        "consolidated_windows",
        ["window_start"],
    )

    # ── hourly_wiki_stats ─────────────────────────────────────
    op.create_table(
        "hourly_wiki_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("total_edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("minor_edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column(
            "total_bytes_added", sa.BigInteger(), nullable=False, server_default="0"
        ),
        sa.Column(
            "total_bytes_removed", sa.BigInteger(), nullable=False, server_default="0"
        ),
        sa.Column("active_users", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active_pages", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "distinct_languages", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="live"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("hour", "source", name="uq_hourly_wiki_stats_hour_source"),
    )
    op.create_index("ix_hourly_wiki_stats_hour", "hourly_wiki_stats", ["hour"])

    # ── hourly_language_stats ─────────────────────────────────
    op.create_table(
        "hourly_language_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=False),
        sa.Column("edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("minor_edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_added", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_removed", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("active_users", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="live"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "hour",
            "language",
            "source",
            name="uq_hourly_language_stats_hour_lang_source",
        ),
    )
    op.create_index(
        "ix_hourly_language_stats_hour", "hourly_language_stats", ["hour"]
    )
    op.create_index(
        "ix_hourly_language_stats_language", "hourly_language_stats", ["language"]
    )
    op.create_index(
        "ix_hourly_language_stats_edits", "hourly_language_stats", ["edits"]
    )

    # ── hourly_page_stats ─────────────────────────────────────
    op.create_table(
        "hourly_page_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("language", sa.String(length=16), nullable=False),
        sa.Column("page_title", sa.String(length=512), nullable=False),
        sa.Column("page_id", sa.BigInteger(), nullable=True),
        sa.Column("edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_added", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_removed", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("distinct_users", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="live"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "hour",
            "language",
            "page_title",
            "source",
            name="uq_hourly_page_stats_hour_lang_page_source",
        ),
    )
    op.create_index("ix_hourly_page_stats_hour", "hourly_page_stats", ["hour"])
    op.create_index(
        "ix_hourly_page_stats_language", "hourly_page_stats", ["language"]
    )
    op.create_index("ix_hourly_page_stats_edits", "hourly_page_stats", ["edits"])
    op.create_index(
        "ix_hourly_page_stats_page_id", "hourly_page_stats", ["page_id"]
    )

    # ── hourly_user_stats ─────────────────────────────────────
    op.create_table(
        "hourly_user_stats",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column("edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("minor_edits", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_added", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("bytes_removed", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("is_bot", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="live"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "hour", "username", "source", name="uq_hourly_user_stats_hour_user_source"
        ),
    )
    op.create_index("ix_hourly_user_stats_hour", "hourly_user_stats", ["hour"])
    op.create_index("ix_hourly_user_stats_edits", "hourly_user_stats", ["edits"])
    op.create_index(
        "ix_hourly_user_stats_is_bot", "hourly_user_stats", ["is_bot"]
    )

    # ── suspicious_edit_summary ───────────────────────────────
    op.create_table(
        "suspicious_edit_summary",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("hour", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "detected_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("language", sa.String(length=16), nullable=False),
        sa.Column("page_title", sa.String(length=512), nullable=False),
        sa.Column("username", sa.String(length=255), nullable=True),
        sa.Column("score", sa.Float(), nullable=False, server_default="0"),
        sa.Column("reason", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("comment", sa.Text(), nullable=True),
        sa.Column("source", sa.String(length=32), nullable=False, server_default="live"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_suspicious_edit_summary_hour", "suspicious_edit_summary", ["hour"]
    )
    op.create_index(
        "ix_suspicious_edit_summary_detected_at",
        "suspicious_edit_summary",
        ["detected_at"],
    )
    op.create_index(
        "ix_suspicious_edit_summary_score", "suspicious_edit_summary", ["score"]
    )
    op.create_index(
        "ix_suspicious_edit_summary_language",
        "suspicious_edit_summary",
        ["language"],
    )

    # ── app_settings ──────────────────────────────────────────
    op.create_table(
        "app_settings",
        sa.Column("key", sa.String(length=128), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade() -> None:
    op.drop_table("app_settings")
    op.drop_index("ix_suspicious_edit_summary_language", table_name="suspicious_edit_summary")
    op.drop_index("ix_suspicious_edit_summary_score", table_name="suspicious_edit_summary")
    op.drop_index("ix_suspicious_edit_summary_detected_at", table_name="suspicious_edit_summary")
    op.drop_index("ix_suspicious_edit_summary_hour", table_name="suspicious_edit_summary")
    op.drop_table("suspicious_edit_summary")
    op.drop_index("ix_hourly_user_stats_is_bot", table_name="hourly_user_stats")
    op.drop_index("ix_hourly_user_stats_edits", table_name="hourly_user_stats")
    op.drop_index("ix_hourly_user_stats_hour", table_name="hourly_user_stats")
    op.drop_table("hourly_user_stats")
    op.drop_index("ix_hourly_page_stats_page_id", table_name="hourly_page_stats")
    op.drop_index("ix_hourly_page_stats_edits", table_name="hourly_page_stats")
    op.drop_index("ix_hourly_page_stats_language", table_name="hourly_page_stats")
    op.drop_index("ix_hourly_page_stats_hour", table_name="hourly_page_stats")
    op.drop_table("hourly_page_stats")
    op.drop_index("ix_hourly_language_stats_edits", table_name="hourly_language_stats")
    op.drop_index("ix_hourly_language_stats_language", table_name="hourly_language_stats")
    op.drop_index("ix_hourly_language_stats_hour", table_name="hourly_language_stats")
    op.drop_table("hourly_language_stats")
    op.drop_index("ix_hourly_wiki_stats_hour", table_name="hourly_wiki_stats")
    op.drop_table("hourly_wiki_stats")
    op.drop_index("ix_consolidated_windows_window_start", table_name="consolidated_windows")
    op.drop_index("ix_consolidated_windows_status", table_name="consolidated_windows")
    op.drop_table("consolidated_windows")
