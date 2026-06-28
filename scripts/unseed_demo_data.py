"""Remove all synthetic seed data (rows where source='synthetic')."""
from __future__ import annotations

import os
import sys
from pathlib import Path

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from sqlalchemy import create_engine, delete  # noqa: E402
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


def main() -> int:
    engine = create_engine(settings.database_url, future=True)
    total = 0
    with Session(engine) as session:
        for model in (
            HourlyWikiStats,
            HourlyLanguageStats,
            HourlyPageStats,
            HourlyUserStats,
            SuspiciousEditSummary,
        ):
            res = session.execute(
                delete(model).where(model.source == SOURCE)
            )
            total += res.rowcount or 0
        session.commit()
    print(f"[unseed] removed {total} synthetic rows")
    return 0


if __name__ == "__main__":
    sys.exit(main())
