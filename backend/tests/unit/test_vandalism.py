from __future__ import annotations

from app.services.vandalism_logic import score_event


def _ev(**overrides):
    base = {
        "bot": False,
        "is_anonymous": False,
        "comment": "regular edit",
        "bytes_added": 10,
        "bytes_removed": 0,
        "event_type": "edit",
        "title": "Some article",
        "user": "TrustedEditor",
    }
    base.update(overrides)
    return base


def test_bot_edits_are_never_suspicious():
    result = score_event(_ev(bot=True, is_anonymous=True, bytes_removed=5000))
    assert result.score == 0.0
    assert result.reasons == []


    result = score_event(_ev(is_anonymous=True, bytes_removed=2500, comment=""))
    assert result.score > 0
    assert "anonymous_editor" in result.reasons
    assert "no_edit_summary" in result.reasons
    assert any(r.startswith("massive_removal") for r in result.reasons)


def test_blanking_adds_blank_reason():
    result = score_event(_ev(is_anonymous=True, bytes_removed=2000, bytes_added=0))
    assert "page_blank" in result.reasons


def test_score_is_capped_at_one():
    result = score_event(
        _ev(
            is_anonymous=True,
            bytes_removed=5000,
            bytes_added=0,
            comment="",
            event_type="new",
            title="PORN vandalism",
        )
    )
    assert result.score <= 1.0
    assert "offensive_pattern" in result.reasons


def test_clean_edit_is_not_suspicious():
    result = score_event(_ev())
    assert result.score == 0.0
