# AGENTS.md

Guidance for AI coding agents working in this repository. This project keeps a
single source of truth in [CLAUDE.md](./CLAUDE.md) — read it for commands,
architecture, and conventions.

Quick orientation:

- WikiPulse: Wikimedia EventStreams → Kafka → Elasticsearch + PostgreSQL, with a
  FastAPI backend and a React (Vite) dashboard.
- Backend tasks run through the root `Makefile` against `.venv` (Python 3.12):
  `make dev`, `make init`, `make test`, `make lint`, `make typecheck`.
- The canonical event path is `producer → wiki.raw → demux → {index, analytics, vandalism}`;
  all Kafka I/O goes through `app/kafka/client.py`; migrations own the schema.
- Don't run `make format` for unrelated changes (it rewrites the whole tree).
  Keep changes surgical and idempotent; never run `purge` targets outside local.

See [CLAUDE.md](./CLAUDE.md) for the full details.
