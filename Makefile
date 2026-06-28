SHELL := /bin/bash

PYTHON ?= python3.12
VENV := .venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

COMPOSE := docker compose
BACKEND_DIR := backend
FRONTEND_DIR := frontend

.DEFAULT_GOAL := help

help:
	@echo "WikiPulse development commands"
	@echo ""
	@echo "Setup:"
	@echo "  make setup              Create venv, install deps, start infra, initialize system"
	@echo "  make venv               Create Python 3.12 virtual environment"
	@echo "  make install            Install backend Python deps"
	@echo ""
	@echo "Docker:"
	@echo "  make up                 Start all services"
	@echo "  make dev                Start all services with rebuild"
	@echo "  make down               Stop services"
	@echo "  make restart            Restart services"
	@echo "  make logs               Tail all logs"
	@echo "  make ps                 Show service status"
	@echo "  make health             Run local healthcheck"
	@echo ""
	@echo "Initialization:"
	@echo "  make init               Run full init: migrations, Kafka topics, Elasticsearch"
	@echo "  make kafka-init         Create Kafka topics"
	@echo "  make es-init            Create Elasticsearch templates and aliases"
	@echo ""
	@echo "Database:"
	@echo "  make migrate            Run Alembic migrations"
	@echo "  make migration name=x   Create migration"
	@echo "  make downgrade          Roll back one migration"
	@echo "  make db-current         Show current migration"
	@echo "  make db-history         Show migration history"
	@echo "  make db-shell           Open psql shell"
	@echo ""
	@echo "Elasticsearch:"
	@echo "  make es-indices         List indices"
	@echo "  make es-aliases         List aliases"
	@echo "  make es-clean-live      Delete expired live indices"
	@echo ""
	@echo "Kafka:"
	@echo "  make kafka-topics       List Kafka topics"
	@echo "  make kafka-ui           Print Kafka UI URL"
	@echo ""
	@echo "Streaming:"
	@echo "  make logs-producer      Tail producer logs"
	@echo "  make logs-indexer       Tail ES indexer logs"
	@echo "  make logs-analytics     Tail analytics consumer logs"
	@echo "  make logs-vandalism     Tail vandalism consumer logs"
	@echo "  make logs-scheduler     Tail scheduler logs"
	@echo "  make restart-pipeline   Restart producer/demux/indexer/analytics/vandalism/scheduler"
	@echo "  make pipeline           Start only the streaming pipeline services"
	@echo ""
	@echo "Observability:"
	@echo "  make obs-up             Start Prometheus + Tempo + Grafana"
	@echo "  make obs-down           Stop the observability stack"
	@echo "  make obs-logs           Tail Prometheus/Tempo/Grafana logs"
	@echo "  make grafana            Print Grafana URL (admin/admin)"
	@echo "Data:"
	@echo "  make seed               Insert synthetic dashboard data"
	@echo "  make unseed             Remove synthetic data"
	@echo "  make consolidate        Run consolidation for latest completed window"
	@echo "  make consolidate-dry-run Run consolidation dry-run"
	@echo "  make consolidate-window window_start=\"2025-01-01T00:00:00\""
	@echo "  make cleanup            Run cleanup jobs"
	@echo ""
	@echo "Quality:"
	@echo "  make test               Run all tests"
	@echo "  make test-unit          Run unit tests"
	@echo "  make test-integration   Run integration tests"
	@echo "  make lint               Run ruff"
	@echo "  make format             Run black and ruff format"
	@echo "  make typecheck          Run mypy"
	@echo ""
	@echo "Danger:"
	@echo "  make purge              Stop services and delete app data (CONFIRM_PURGE=1)"
	@echo "  make purge-volumes      Stop services and delete Docker volumes"
	@echo "  make purge-python       Delete local venv and caches"

setup: venv install dev init

venv:
	$(PYTHON) -m venv $(VENV)

install:
	$(PIP) install --upgrade pip
	$(PIP) install -r $(BACKEND_DIR)/requirements.txt
	$(PIP) install -r $(BACKEND_DIR)/requirements-dev.txt

up:
	$(COMPOSE) up -d

dev:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f --tail=200

logs-producer:
	$(COMPOSE) logs -f --tail=200 producer

logs-indexer:
	$(COMPOSE) logs -f --tail=200 indexer

logs-analytics:
	$(COMPOSE) logs -f --tail=200 analytics

logs-vandalism:
	$(COMPOSE) logs -f --tail=200 vandalism

logs-scheduler:
	$(COMPOSE) logs -f --tail=200 scheduler

restart-pipeline:
	$(COMPOSE) restart producer demux indexer analytics vandalism scheduler

pipeline:
	$(COMPOSE) up -d producer demux indexer analytics vandalism scheduler

obs-up:
	$(COMPOSE) up -d prometheus tempo grafana

obs-down:
	$(COMPOSE) stop prometheus tempo grafana

obs-logs:
	$(COMPOSE) logs -f --tail=200 prometheus tempo grafana

grafana:
	@echo "Grafana:  http://localhost:3000  (admin / admin)"

prometheus:
	@echo "Prometheus: http://localhost:9090  (targets: /targets)"

tempo:
	@echo "Tempo:    http://localhost:3200  (query API)"

ps:
	$(COMPOSE) ps

health:
	$(PY) scripts/dev_healthcheck.py

init:
	$(PY) scripts/init_all.py

migrate:
	cd $(BACKEND_DIR) && ../$(PY) -m alembic upgrade head

migration:
	cd $(BACKEND_DIR) && ../$(PY) -m alembic revision --autogenerate -m "$(name)"

downgrade:
	cd $(BACKEND_DIR) && ../$(PY) -m alembic downgrade -1

db-current:
	cd $(BACKEND_DIR) && ../$(PY) -m alembic current

db-history:
	cd $(BACKEND_DIR) && ../$(PY) -m alembic history

db-shell:
	$(COMPOSE) exec postgres psql -U $${POSTGRES_USER:-wikipulse} -d $${POSTGRES_DB:-wikipulse}

kafka-init:
	$(PY) scripts/init_kafka_topics.py

kafka-topics:
	$(COMPOSE) exec kafka kafka-topics --bootstrap-server kafka:9092 --list

kafka-ui:
	@echo "Kafka UI: http://localhost:8080"

es-init:
	$(PY) scripts/init_elasticsearch.py

es-indices:
	curl -s http://localhost:9200/_cat/indices?v

es-aliases:
	curl -s http://localhost:9200/_cat/aliases?v

es-clean-live:
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.cleanup_live_data

seed:
	$(PY) scripts/seed_demo_data.py

unseed:
	$(PY) scripts/unseed_demo_data.py

test:
	cd $(BACKEND_DIR) && ../$(PY) -m pytest

test-unit:
	cd $(BACKEND_DIR) && ../$(PY) -m pytest tests/unit

test-integration:
	cd $(BACKEND_DIR) && ../$(PY) -m pytest tests/integration

lint:
	cd $(BACKEND_DIR) && ../$(PY) -m ruff check .

format:
	cd $(BACKEND_DIR) && ../$(PY) -m black .
	cd $(BACKEND_DIR) && ../$(PY) -m ruff check . --fix

typecheck:
	cd $(BACKEND_DIR) && ../$(PY) -m mypy app

consolidate:
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.consolidate_3h

consolidate-dry-run:
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.consolidate_3h --dry-run

consolidate-window:
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.consolidate_3h --window-start "$(window_start)"

cleanup:
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.cleanup_live_data
	cd $(BACKEND_DIR) && ../$(PY) -m app.jobs.cleanup_old_aggregates

purge:
	bash scripts/purge_local.sh

purge-volumes:
	$(COMPOSE) down -v --remove-orphans

purge-python:
	rm -rf $(VENV)
	find . -type d -name "__pycache__" -prune -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -prune -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -prune -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -prune -exec rm -rf {} +

.PHONY: help setup venv install up dev down restart logs ps health init migrate migration \
        downgrade db-current db-history db-shell kafka-init kafka-topics kafka-ui es-init \
        es-indices es-aliases es-clean-live seed unseed test test-unit test-integration \
        lint format typecheck consolidate consolidate-dry-run consolidate-window cleanup \
        logs-producer logs-indexer logs-analytics logs-vandalism logs-scheduler \
        restart-pipeline pipeline obs-up obs-down obs-logs grafana prometheus tempo \
        purge purge-volumes purge-python
