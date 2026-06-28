# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

WikiPulse ingests the public Wikimedia EventStreams feed through an event-driven
pipeline (Kafka → Elasticsearch + PostgreSQL) and serves live search, hourly
analytics, vandalism candidates, and a WebSocket-driven dashboard.
Stack: FastAPI + PostgreSQL + Apache Kafka + Elasticsearch + React (Vite).

## Commands

All backend commands run through the repo-root `Makefile` against the
`.venv` Python (3.12). Copy `cp .env.example .env` first; `make setup` then
builds the venv, starts Docker services, and runs full init.

- `make dev` / `make up` — start all services (rebuild / no rebuild)
- `make init` — migrations + Kafka topics + Elasticsearch template/aliases (idempotent)
- `make health` — local healthcheck script; `/health` endpoint is the source of truth
- `make logs-<svc>` — tail a single pipeline service (`producer`, `indexer`, `analytics`, `vandalism`, `scheduler`)
- `make restart-pipeline` — restart producer/demux/indexer/analytics/vandalism/scheduler

Quality (all run from `backend/` via the venv):

- `make test` / `make test-unit` / `make test-integration`
- Single test: `cd backend && ../.venv/bin/python -m pytest tests/unit/test_vandalism.py::test_name`
- `make lint` (ruff) · `make typecheck` (mypy `app`)
- `make format` runs black + `ruff --fix`. **Do not run `make format` for unrelated changes** — it can touch the whole tree.

Migrations own the schema; the app never creates tables at runtime.

- `make migration name="..."` (autogenerate) · `make migrate` · `make downgrade`
- Integration tests (`tests/integration`) require live Docker services; unit tests do not.

Frontend (`frontend/`): `npm run dev` / `npm run build`. Vite proxies `/api`,
`/health`, `/ws` to `backend-api:8000`.

## Architecture

**One canonical event path, fanned out after Kafka.** The producer is the only
component that touches Wikimedia and writes a single stream `wiki.raw` (invalid
payloads → `wiki.deadletter`). `demux` fans `wiki.raw` out into one topic per
concern so each sink is independently restartable/replayable:

- `wiki.index` → `indexer` → Elasticsearch (live search)
- `wiki.analytics` → `analytics` → PostgreSQL hourly aggregates
- `wiki.vandalism` → `vandalism` → PostgreSQL suspicious-edit candidates
- `scheduler` → periodic jobs (hourly rollover, reconcile, 3h consolidation, cleanup)
- `backend-api` runs an in-process best-effort consumer (`live_broadcast`) → WebSocket hub → dashboard

**Delivery semantics.** At-least-once everywhere; exactly-once is intentionally
not attempted. Each consumer commits offsets only after its store write
succeeds, and every sink is idempotent: indexer keys on `event_id`, analytics
upserts are additive, vandalism is append-only. `enable.auto.commit` is off by
default (`live_broadcast` is the exception — it's a disposable tail).

**Key module boundaries:**

- `app/kafka/client.py` — *all* Kafka I/O goes through here (serialize/produce/consume/commit). Don't import `confluent_kafka` elsewhere. Messages are keyed on `language` for partition affinity/ordering.
- `app/core/config.py` — a plain `Settings` class (not pydantic) so standalone `scripts/` can import it outside the FastAPI process. `settings` is the singleton.
- `app/services/runtime.py` — shared signal-handling / `should_stop()` loop control for every long-running consumer; consumer `main()`s follow the same shape.
- `app/services/health.py` — read-only checks; reports `migrations: not_initialized` rather than ever creating schema.
- `app/search/` — `index_templates.py`, `aliases.py`, `writer.py`. Live events use two aliases: `wiki-live-events-write` (one active hourly index) and `wiki-live-events-read` (all retained indices); cleanup removes from the read alias before deleting.
- `app/models/__init__.py` — ORM mirrors the Alembic-owned schema. `source` column ('live' | 'synthetic' | consolidated) is part of most unique constraints; analytics aggregates are keyed by `(hour, ..., source)`.
- `app/jobs/` — `consolidate_3h`, `reconcile_live`, `cleanup_live_data`, `cleanup_old_aggregates`; each is a runnable module (`python -m app.jobs.X`) the scheduler also calls.

**Adding a pipeline stage:** add the topic to `settings.kafka_topics` + config,
fan out in `demux.py`, write a consumer under `app/services/` using the
`runtime` loop and `app/kafka` helpers, and register it in `docker-compose.yml`.

## Conventions

- ruff + black, line length 100, target py312. ruff selects E/F/W/I/UP/B/SIM; `migrations/versions/*` is excluded from all tooling.
- Init scripts under `scripts/` are idempotent and safe to re-run.
- `make purge` / `make purge-volumes` delete local data — never run against anything but local volumes.
