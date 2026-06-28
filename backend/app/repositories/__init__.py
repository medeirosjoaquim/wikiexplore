"""Repository layer — data-access only, no business logic."""
from __future__ import annotations

from app.repositories.aggregates import apply_events

__all__ = ["apply_events"]
