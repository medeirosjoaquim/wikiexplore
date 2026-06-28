#!/usr/bin/env bash
# Local purge: stop services and delete local app data + Python caches.
#
# Requires either CONFIRM_PURGE=1 in the environment or interactive
# confirmation at the prompt. Never deletes .env, source code, migrations,
# the README, or the frontend source.

set -euo pipefail

if [[ "${CONFIRM_PURGE:-0}" != "1" ]]; then
  echo "This will stop all services and delete local application data."
  echo "Re-run with CONFIRM_PURGE=1 or confirm below."
  read -r -p "Type PURGE to confirm: " reply
  if [[ "$reply" != "PURGE" ]]; then
    echo "Aborted."
    exit 1
  fi
fi

cd "$(dirname "$0")/.."

echo "[purge] stopping services (volumes removed)..."
docker compose down -v --remove-orphans || true

echo "[purge] removing local .data..."
rm -rf ./.data

echo "[purge] removing python caches..."
rm -rf ./backend/.pytest_cache ./backend/.mypy_cache ./backend/.ruff_cache
find . -type d -name "__pycache__" -prune -exec rm -rf {} + || true

echo "[purge] complete."
echo "Preserved: .env, source, migrations, README, frontend."
