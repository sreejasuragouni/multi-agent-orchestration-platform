#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"

docker compose up -d --build
docker compose up -d research_agent analysis_agent code_agent

echo "== compose ps =="
docker compose ps

echo "== waiting for API /health =="
for i in {1..60}; do
  if curl -sf "${API_BASE}/health" >/dev/null; then
    echo "API is up ✅"
    break
  fi
  sleep 2
done

echo "== API health =="
curl -s -i "${API_BASE}/health" | head -n 20 || true
echo

echo "== API ready =="
curl -s -i "${API_BASE}/ready" | head -n 30 || true
echo

# If still not reachable, dump logs and fail
if ! curl -sf "${API_BASE}/health" >/dev/null; then
  echo "ERROR: API not reachable at ${API_BASE}"
  docker compose logs --tail 150 api || true
  exit 1
fi