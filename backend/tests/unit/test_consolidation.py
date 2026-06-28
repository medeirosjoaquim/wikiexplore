from __future__ import annotations

import datetime as _dt

from app.jobs.consolidate_3h import latest_completed_window


def test_latest_completed_window_aligns_to_window_size(monkeypatch):
    settings_window = 3
    import app.jobs.consolidate_3h as mod

    monkeypatch.setattr(mod.settings, "consolidation_window_hours", settings_window, raising=False)
    now = _dt.datetime(2025, 1, 1, 6, 45, tzinfo=_dt.UTC)
    start, end = latest_completed_window(now)
    assert start == _dt.datetime(2025, 1, 1, 3, 0, tzinfo=_dt.UTC)
    assert end == _dt.datetime(2025, 1, 1, 6, 0, tzinfo=_dt.UTC)


def test_models_have_source_column():
    from app.models import (
        HourlyLanguageStats,
        HourlyPageStats,
        HourlyUserStats,
        HourlyWikiStats,
        SuspiciousEditSummary,
    )

    for model in (
        HourlyWikiStats,
        HourlyLanguageStats,
        HourlyPageStats,
        HourlyUserStats,
        SuspiciousEditSummary,
    ):
        assert "source" in model.__table__.columns
