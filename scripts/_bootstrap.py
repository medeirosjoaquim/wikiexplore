"""Shared import bootstrap for scripts.

Inserts the backend package onto sys.path so scripts can `import app.*`
regardless of whether they run on the host (scripts/ next to backend/) or
inside the container (/app/scripts next to /app, or scripts mounted at
/app/scripts with backend code already importable).
"""
from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent


def add_backend_to_path() -> None:
    # When scripts live at <root>/scripts, backend is at <root>/backend.
    root_sibling_backend = _HERE.parent / "backend"
    # Inside the container backend code may already be importable from /app.
    container_app = Path("/app")
    for candidate in (root_sibling_backend, container_app):
        if (candidate / "app" / "core" / "config.py").exists():
            if str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))
            return
    # Fallback: assume cwd-relative backend.
    cwd_backend = Path.cwd() / "backend"
    if cwd_backend.exists() and str(cwd_backend) not in sys.path:
        sys.path.insert(0, str(cwd_backend))
