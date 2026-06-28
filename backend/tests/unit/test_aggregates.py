from __future__ import annotations

from app.repositories.aggregates import build_rows


def _ev(**overrides):
    base = {
        "timestamp": "2025-06-01T10:30:00Z",
        "language": "en",
        "user": "Alice",
        "title": "Wikipedia",
        "page_id": 1,
        "minor": False,
        "bot": False,
        "bytes_added": 100,
        "bytes_removed": 0,
    }
    base.update(overrides)
    return base


def test_build_rows_dedupes_by_hour_within_a_batch():
    # Two events in the same hour collapse the wiki/lang/page batches to one row.
    wiki, lang, page, user = build_rows([_ev(), _ev(user="Bob")])
    assert len(wiki) == 1
    assert len(lang) == 1
    assert len(page) == 1
    assert len(user) == 2  # distinct users
    # Aggregated counts are summed.
    assert wiki[0]["total_edits"] == 2


def test_build_rows_floors_hour_and_sums():
    wiki, _, _, _ = build_rows([_ev(timestamp="2025-06-01T10:59:59Z", minor=True, bytes_added=50), _ev(bytes_added=30)])
    row = wiki[0]
    assert row["hour"].minute == 0
    assert row["total_edits"] == 2
    assert row["minor_edits"] == 1
    assert row["total_bytes_added"] == 80
    assert row["source"] == "live"


def test_build_rows_clamps_negative_and_uses_removal():
    wiki, _, _, _ = build_rows([_ev(bytes_added=0, bytes_removed=250)])
    assert wiki[0]["total_bytes_added"] == 0
    assert wiki[0]["total_bytes_removed"] == 250


def test_build_rows_separates_languages_and_pages():
    _, lang, page, _ = build_rows([_ev(language="en", title="A"), _ev(language="fr", title="B")])
    assert {r["language"] for r in lang} == {"en", "fr"}
    assert {r["page_title"] for r in page} == {"A", "B"}


def test_build_rows_aggregates_repeated_keys_without_duplicates():
    # Simulates the high-volume case: many edits to the same page in one batch.
    events = [_ev() for _ in range(50)] + [_ev(language="fr") for _ in range(10)]
    wiki, lang, page, _ = build_rows(events)
    assert len(wiki) == 1  # one hour, one source
    assert wiki[0]["total_edits"] == 60
    assert {r["language"] for r in lang} == {"en", "fr"}
    assert lang[0]["edits"] + lang[1]["edits"] == 60
    assert page[0]["edits"] == 50


def test_build_rows_empty_input():
    wiki, lang, page, user = build_rows([])
    assert wiki == lang == page == user == []
