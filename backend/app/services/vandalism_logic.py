"""Heuristic vandalism scoring for Wikipedia edits.

Pure, deterministic, side-effect-free — the consumer is a thin Kafka/PG shell
around :func:`score_event`, which is unit-tested directly.

This is intentionally a transparent rules engine (not ML): the dashboard shows
the contributing reasons, and the rules are cheap to run on every event. A
score >= ``VANDALISM_THRESHOLD`` is persisted to ``suspicious_edit_summary``.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# ── signal weights ──────────────────────────────────────────────────────────
W_ANON = 0.30
W_NO_COMMENT = 0.20
W_LARGE_REMOVAL = 0.25
W_MASSIVE_REMOVAL = 0.40
W_BLANKING = 0.35
W_ANON_NEW_PAGE = 0.20
W_OFFENSIVE = 0.30

LARGE_REMOVAL_BYTES = 500
MASSIVE_REMOVAL_BYTES = 2000

# A small, deliberately conservative offensive-pattern list. We only flag
# obvious vandalism markers; false positives are worse than false negatives.
OFFENSIVE_PATTERN = re.compile(
    r"(?i)\b(porn|xxx|penis|vagina|fuck|shit|bitch|nigg|faggot|cunt|whore|nazi)\b"
)

# Titles/users that are ALL-CAPS gibberish or repetitive padding.
GIBBERISH_PATTERN = re.compile(r"^(?:[A-Z\W\d]{8,}|(.)\1{5,})$")


@dataclass(slots=True)
class ScoreResult:
    score: float
    reasons: list[str] = field(default_factory=list)

    @property
    def suspicious(self) -> bool:
        return self.score > 0.0


def score_event(event: dict) -> ScoreResult:
    """Return a 0..1 suspiciousness score plus human-readable reasons."""
    if event.get("bot"):
        return ScoreResult(0.0)

    score = 0.0
    reasons: list[str] = []

    is_anon = bool(event.get("is_anonymous"))
    if is_anon:
        score += W_ANON
        reasons.append("anonymous_editor")

    comment = str(event.get("comment") or "").strip()
    if not comment:
        score += W_NO_COMMENT
        reasons.append("no_edit_summary")

    removed = int(event.get("bytes_removed") or 0)
    added = int(event.get("bytes_added") or 0)
    if removed >= MASSIVE_REMOVAL_BYTES:
        score += W_MASSIVE_REMOVAL
        reasons.append(f"massive_removal:{removed}b")
    elif removed >= LARGE_REMOVAL_BYTES:
        score += W_LARGE_REMOVAL
        reasons.append(f"large_removal:{removed}b")

    # Blanking: large removal with (near) zero new content.
    if removed >= LARGE_REMOVAL_BYTES and added == 0:
        score += W_BLANKING
        reasons.append("page_blank")

    event_type = str(event.get("event_type") or "")
    if is_anon and event_type == "new":
        score += W_ANON_NEW_PAGE
        reasons.append("anon_new_page")

    haystack = f"{event.get('title', '')} {event.get('user', '')} {comment}"
    if OFFENSIVE_PATTERN.search(haystack):
        score += W_OFFENSIVE
        reasons.append("offensive_pattern")
    elif GIBBERISH_PATTERN.match(str(event.get("title") or "")):
        score += 0.10
        reasons.append("gibberish_title")

    return ScoreResult(min(score, 1.0), reasons)
