"""Insert a small amount of clearly-marked synthetic data so the dashboard
is not empty before real data arrives.

Every seeded row carries source='synthetic'. Run `make unseed` to remove.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import os
import random
import sys
from pathlib import Path

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from sqlalchemy import create_engine, select  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.models import (  # noqa: E402
    HourlyLanguageStats,
    HourlyPageStats,
    HourlyUserStats,
    HourlyWikiStats,
    SuspiciousEditSummary,
)

SOURCE = "synthetic"
LANGUAGES = ["en", "de", "fr", "es", "ja", "ru", "zh", "pt", "it", "ar"]
PAGE_TITLES = [
    "Wikipedia", "Python_(programming_language)", "World_War_II",
    "Climate_change", "Albert_Einstein", "Linux", "Moon_landing",
    "Quantum_mechanics", "Roman_Empire", "Internet",
]
USERNAMES = [
    "EditorOne", "WikiFan", "AnonymousBot", "CleanupCrew", "HistBuff",
    "GrammarGuru", "DataDude", "NightOwl", "SourceSleuth", "NewbieEditor",
]


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _hour_floor(dt: _dt.datetime) -> _dt.datetime:
    return dt.replace(minute=0, second=0, microsecond=0)


def seed(session: Session, hours: int = 24) -> int:
    now = _hour_floor(_utc_now())
    inserted = 0
    rng = random.Random(42)

    for h in range(hours):
        hour = now - _dt.timedelta(hours=h)
        # wiki-wide stats
        session.add(
            HourlyWikiStats(
                hour=hour,
                total_edits=rng.randint(800, 2400),
                minor_edits=rng.randint(100, 600),
                total_bytes_added=rng.randint(200_000, 800_000),
                total_bytes_removed=rng.randint(50_000, 300_000),
                active_users=rng.randint(200, 700),
                active_pages=rng.randint(400, 1200),
                distinct_languages=len(LANGUAGES),
                source=SOURCE,
            )
        )
        inserted += 1

        for lang in LANGUAGES:
            edits = rng.randint(40, 400)
            session.add(
                HourlyLanguageStats(
                    hour=hour,
                    language=lang,
                    edits=edits,
                    minor_edits=edits // 4,
                    bytes_added=edits * rng.randint(80, 400),
                    bytes_removed=edits * rng.randint(20, 150),
                    active_users=max(3, edits // 8),
                    source=SOURCE,
                )
            )
            inserted += 1

        for page in PAGE_TITLES:
            edits = rng.randint(1, 30)
            session.add(
                HourlyPageStats(
                    hour=hour,
                    language=rng.choice(LANGUAGES),
                    page_title=page,
                    page_id=rng.randint(1_000_000, 9_999_999),
                    edits=edits,
                    bytes_added=edits * rng.randint(50, 600),
                    bytes_removed=edits * rng.randint(10, 200),
                    distinct_users=max(1, edits // 3),
                    source=SOURCE,
                )
            )
            inserted += 1

        for user in USERNAMES:
            edits = rng.randint(1, 25)
            session.add(
                HourlyUserStats(
                    hour=hour,
                    username=user,
                    edits=edits,
                    minor_edits=edits // 5,
                    bytes_added=edits * rng.randint(60, 500),
                    bytes_removed=edits * rng.randint(5, 120),
                    is_bot=user.endswith("Bot"),
                    source=SOURCE,
                )
            )
            inserted += 1

        # occasional suspicious edit
        if rng.random() < 0.4:
            session.add(
                SuspiciousEditSummary(
                    hour=hour,
                    language=rng.choice(LANGUAGES),
                    page_title=rng.choice(PAGE_TITLES),
                    username=rng.choice(USERNAMES),
                    score=round(rng.uniform(0.5, 0.99), 3),
                    reason=rng.choice(
                        ["large_delete", "repeated_edits", "blanking", "anon_high_volume"]
                    ),
                    comment="[synthetic] seeded suspicious edit",
                    source=SOURCE,
                )
            )
            inserted += 1

    session.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed synthetic dashboard data")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args()

    engine = create_engine(settings.database_url, future=True)
    with Session(engine) as session:
        count = seed(session, hours=args.hours)
    print(f"[seed] inserted {count} synthetic rows (source='{SOURCE}')")
    return 0


if __name__ == "__main__":
    sys.exit(main())
