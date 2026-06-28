# WikiPulse

Real-time Wikipedia edit monitoring: ingests the public Wikimedia EventStreams
feed through an event-driven pipeline (Kafka → Elasticsearch + PostgreSQL),
with live search, hourly analytics, 3-hour consolidation, heuristic vandalism
detection, and a professional observability dashboard.

Stack: **FastAPI + PostgreSQL + Apache Kafka + Elasticsearch + React (Vite)**.

---

## Quick start

```bash
cp .env.example .env
make setup
```

`make setup` creates a Python 3.12 venv, installs backend deps, builds and
starts all services, and runs the full initialization (migrations, Kafka
topics, Elasticsearch template + aliases).

Then check it's alive:

```bash
make logs
make health
```

| Service        | URL                          |
| -------------- | ---------------------------- |
| Frontend       | http://localhost:5173        |
| Backend API    | http://localhost:8000        |
| API Docs       | http://localhost:8000/docs   |
| Health         | http://localhost:8000/health |
| Kafka UI       | http://localhost:8080        |
| Kibana         | http://localhost:5601        |
| Elasticsearch  | http://localhost:9200        |

---

## One-command setup guarantee

A fresh checkout becomes a working system with a single command because
`make setup` triggers `make dev` (which starts the `backend-init` service
that waits for healthy Postgres/Kafka/Elasticsearch, runs Alembic, creates
topics, and applies the index template + live aliases).

You can re-run any step independently:

```bash
make init          # full init: migrations + kafka + elasticsearch
make migrate       # alembic upgrade head
make migration name="add foo column"
make downgrade     # roll back one revision
make db-current
make db-history
make kafka-init
make es-init
make health
```

## Architecture notes

* **Event-driven pipeline.** Every edit flows through one canonical path and
  fans out after Kafka, so each sink is independently restartable/replayable:

  ```
  Wikimedia EventStreams (SSE)
    │
    ▼
  producer ──► wiki.raw ──► demux ──┬──► wiki.index    ──► indexer     ──► Elasticsearch (live search)
                                   ├──► wiki.analytics ──► analytics   ──► PostgreSQL (hourly aggregates)
                                   └──► wiki.vandalism ──► vandalism   ──► PostgreSQL (suspicious edits)
                                                                scheduler ──► hourly rollover + reconcile + consolidation + cleanup
                                              backend-api (in-process live consumer) ──► WebSocket hub ──► dashboard
  ```

  The producer is the only component that touches Wikimedia; invalid payloads
  go to `wiki.deadletter`. Each consumer commits offsets only after its store
  write succeeds, so failures replay idempotently (indexer keys on `event_id`;
  analytics upserts are additive; vandalism is an append-only candidate feed).

* **Migrations own the schema.** The application never creates tables at
  runtime. If migrations are missing, `/health` reports
  `migrations: not_initialized` instead of silently creating them.
* **Elasticsearch live events** use two aliases:
  `wiki-live-events-write` → exactly one active hourly index (producers write here);
  `wiki-live-events-read` → all retained hourly indices (search reads here).
  The cleanup job removes old indices from the read alias before deleting them.
* **Index template** `wiki-live-events-template` matches `wiki-live-events-*`
  with the explicit mapping, single shard, 0 replicas, 5s refresh,
  `best_compression`.
* **Kafka topics** (created by `scripts/init_kafka_topics.py`, idempotent):

  | topic           | partitions |
  | --------------- | ---------- |
  | `wiki.raw`      | 3          |
  | `wiki.index`    | 3          |
  | `wiki.analytics`| 3          |
  | `wiki.vandalism`| 2          |
  | `wiki.deadletter`| 1         |

  retention 6h, `cleanup.policy=delete`, `compression.type=producer`.

* **All init scripts are idempotent** and safe to run repeatedly.

---

## Observability (Prometheus + Tempo + Grafana)

Every service is instrumented with Prometheus metrics and OpenTelemetry
traces, with **W3C trace context propagated through Kafka headers** so a single
trace spans the whole pipeline:

```
producer (SSE) -> wiki.raw -> demux -> { indexer -> ES, analytics -> PG, vandalism -> PG }
```

```bash
make obs-up        # start Prometheus + Tempo + Grafana (already in `make dev`)
make grafana       # http://localhost:3000  (admin / admin)
make prometheus    # http://localhost:9090  (targets at /targets)
make tempo         # http://localhost:3200  (Tempo query API)
```

| Service        | URL                          |
| -------------- | ---------------------------- |
| Grafana        | http://localhost:3000        |
| Prometheus     | http://localhost:9090        |
| Tempo (query)  | http://localhost:3200        |
| API /metrics   | http://localhost:8000/metrics |

Grafana auto-provisions the Prometheus + Tempo datasources and a
"WikiPulse — Pipeline Overview" dashboard (throughput, HTTP p50/p95, stage
latency p95, batch sizes, vandalism flag rate, scheduler runs, WS clients).

Metrics ports (one prometheus_client HTTP listener per streaming service):
`api :8000`, `producer :9101`, `demux :9102`, `indexer :9103`,
`analytics :9104`, `vandalism :9105`, `scheduler :9106`.

Key metric families: `wikipulse_producer_events_total`,
`wikipulse_indexer_events_total`, `wikipulse_analytics_events_total`,
`wikipulse_vandalism_events_total`, `wikipulse_event_processing_seconds`,
`wikipulse_http_request_duration_seconds`, `wikipulse_bulk_index_batch_size`,
`wikipulse_upsert_batch_size`, `wikipulse_ws_connected_clients`.

Traces export over OTLP/HTTP to Tempo (`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`).
Disable either layer with `OTEL_ENABLED=false` / `METRICS_ENABLED=false`.

---

## Seed data

```bash
make seed     # insert clearly-marked synthetic rows (source='synthetic')
make unseed   # remove all synthetic rows
```

Seeded data covers the last 24h of `hourly_wiki_stats`, top languages,
pages, users, and a few suspicious-edit summaries — just enough so the
dashboard isn't empty before real data arrives.

---

## Consolidation & cleanup

```bash
make consolidate             # roll up the latest completed 3h window
make consolidate-dry-run
make consolidate-window window_start="2025-01-01T00:00:00"
make cleanup                 # prune live indices + old aggregates
make es-clean-live           # delete expired live ES indices only
```

---

## Quality

```bash
make test
make test-unit
make test-integration
make lint
make format
make typecheck
```

---

## Environment files

| File                  | Purpose                                    |
| --------------------- | ------------------------------------------ |
| `.env.example`        | Template — copy to `.env`                  |
| `.env.local.example`  | Host-accessible endpoints (outside docker) |
| `.env.test.example`   | Test config                                |

The app loads `.env`. Never commit real secrets.

---

## VPS / Production flow

```bash
cp .env.example .env
nano .env
make dev
make init
```

> **VPS warning:** Before running on a small VPS, review
> `LIVE_RETENTION_HOURS`, Kafka retention bytes, Elasticsearch heap size
> (`ES_JAVA_OPTS`), and PostgreSQL retention days.

---

## Purging local data

> `make purge` and `make purge-volumes` delete local application data.
> Do not run them on production unless you intentionally want to erase
> local volumes.

```bash
CONFIRM_PURGE=1 make purge   # stop services, delete volumes + .data + caches
make purge-volumes           # docker volumes only
make purge-python            # venv + caches only
```

Purge never deletes `.env`, source code, migrations, the README, or the
frontend source.

---

## Makefile reference

Run `make` (or `make help`) for the full command list.

---

## Project layout

```
scripts/
  wait_for_postgres.py
  wait_for_elasticsearch.py
  wait_for_kafka.py
  init_kafka_topics.py
  init_elasticsearch.py
  init_all.py
  purge_local.sh
  dev_healthcheck.py
  seed_demo_data.py
  unseed_demo_data.py
backend/
  alembic.ini
  migrations/
    env.py
    versions/
      0001_initial.py
  app/
    core/config.py
    database/{base.py,session.py}
    events/{models.py,validation.py}        # canonical event + Wikimedia normalization
    kafka/client.py                          # confluent-kafka produce/consume
    models/__init__.py
    repositories/aggregates.py               # PG upsert increments (additive)
    schemas/__init__.py                      # typed API responses
    search/{index_templates.py,aliases.py,writer.py}
    api/{analytics.py,live.py,websocket.py}
    services/{producer,demux,indexer,analytics,vandalism,scheduler,live_broadcast,vandalism_logic,runtime,health}.py
    jobs/{consolidate_3h,reconcile_live,cleanup_live_data,cleanup_old_aggregates}.py
    main.py
frontend/
  src/{App.jsx,api.js,styles.css,components/*.jsx}
docker-compose.yml
Makefile
