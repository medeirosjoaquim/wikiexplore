"""SQLAlchemy declarative base. All ORM models inherit from `Base`."""
from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Project-wide declarative base."""
