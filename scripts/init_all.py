"""Run the full initialization sequence for WikiPulse.

Order:
  1. wait for PostgreSQL
  2. wait for Kafka
  3. wait for Elasticsearch
  4. run Alembic migrations
  5. create Kafka topics
  6. create ES template + aliases
  7. final health check

Exits non-zero on the first failure.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))  # so `from _bootstrap import ...` works

from _bootstrap import add_backend_to_path  # noqa: E402

add_backend_to_path()

from app.core.config import settings  # noqa: E402


def _find_backend_dir() -> Path:
    """Locate the directory that contains ``alembic.ini`` (the backend root).

    Handles host layout (<root>/backend) and container layout (/app).
    """
    candidates = [
        HERE.parent / "backend",       # host: scripts/ -> ../backend
        HERE.parent,                   # container: /app/scripts -> /app
        Path("/app"),
    ]
    for c in candidates:
        if (c / "alembic.ini").exists():
            return c
    # Fallback: walk up from here.
    node = HERE
    for _ in range(5):
        if (node / "alembic.ini").exists():
            return node
        node = node.parent
    raise RuntimeError("Could not locate backend directory (alembic.ini missing)")


BACKEND_DIR = _find_backend_dir()


def _log(msg: str) -> None:
    print(f"[init] {msg}", flush=True)


def run(cmd: list[str], cwd: Path | None = None, label: str | None = None) -> None:
    label = label or " ".join(cmd)
    _log(f"running: {label}")
    proc = subprocess.run(cmd, cwd=cwd)
    if proc.returncode != 0:
        _log(f"FAILED ({proc.returncode}): {label}")
        sys.exit(proc.returncode)


def main() -> int:
    start = time.time()
    python = sys.executable

    # 1. Wait for PostgreSQL.
    _log("waiting for postgres...")
    rc = subprocess.call([python, str(HERE / "wait_for_postgres.py"), "--timeout", "120"])
    if rc != 0:
        _log("postgres did not become ready")
        return rc
    _log("postgres ready")

    # 2. Wait for Kafka.
    _log("waiting for kafka...")
    rc = subprocess.call([python, str(HERE / "wait_for_kafka.py"), "--timeout", "120"])
    if rc != 0:
        _log("kafka did not become ready")
        return rc
    _log("kafka ready")

    # 3. Wait for Elasticsearch.
    _log("waiting for elasticsearch...")
    rc = subprocess.call(
        [python, str(HERE / "wait_for_elasticsearch.py"), "--timeout", "180"]
    )
    if rc != 0:
        _log("elasticsearch did not become ready")
        return rc
    _log("elasticsearch ready")

    # 4. Run Alembic migrations.
    _log("running migrations...")
    run(
        [python, "-m", "alembic", "upgrade", "head"],
        cwd=str(BACKEND_DIR),
        label="alembic upgrade head",
    )
    _log("migrations complete")

    # 5. Kafka topics.
    _log("creating kafka topics...")
    run([python, str(HERE / "init_kafka_topics.py")], label="init_kafka_topics")
    _log("kafka topics ready")

    # 6. Elasticsearch template + aliases.
    _log("creating elasticsearch template...")
    run([python, str(HERE / "init_elasticsearch.py")], label="init_elasticsearch")
    _log("elasticsearch ready")

    # 7. Final health check.
    _log("running final health check...")
    rc = subprocess.call([python, str(HERE / "dev_healthcheck.py"), "--quiet"])
    if rc != 0:
        _log("final health check reported problems")
        return rc

    elapsed = time.time() - start
    _log(f"initialization complete ({elapsed:.1f}s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
