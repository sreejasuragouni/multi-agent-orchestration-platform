#!/usr/bin/env bash
set -euo pipefail

API_BASE="${API_BASE:-http://localhost:8000}"
TIMEOUT_S="${TIMEOUT_S:-240}"
SLEEP_S="${SLEEP_S:-2}"

TASK_TEXT="${1:-Smoke test: split into research, analysis, code and return final summary.}"

fetch() {
  # Prints: BODY then newline then __HTTP_CODE__=XYZ then newline
  curl -s -w "\n__HTTP_CODE__=%{http_code}\n" "$1" || true
}

get_code() {
  awk -F= '/__HTTP_CODE__/ {print $2}' | tail -n 1 | tr -d '\r'
}

get_body() {
  sed '/__HTTP_CODE__/,$d'
}

ctype() {
  curl -s -D - "$1" -o /dev/null | egrep -i "HTTP/|content-type|server" | tr -d '\r' || true
}

echo "[smoke] posting task to ${API_BASE}/tasks"
RESP="$(curl -s -X POST "${API_BASE}/tasks" -H "Content-Type: application/json" -d "{\"task\":\"${TASK_TEXT}\"}")"
TASK_ID="$(echo "$RESP" | jq -r '.task_id // empty')"

if [[ -z "${TASK_ID}" ]]; then
  echo "[smoke] ERROR: task_id missing. response=${RESP}"
  exit 1
fi

echo "[smoke] task_id=${TASK_ID}"

DEADLINE=$(( $(date +%s) + TIMEOUT_S ))

echo "[smoke] polling status until final is completed (timeout ${TIMEOUT_S}s)"
while true; do
  NOW=$(date +%s)
  if [[ $NOW -ge $DEADLINE ]]; then
    echo "[smoke] TIMEOUT waiting for final."
    echo "[smoke] last status headers:"
    ctype "${API_BASE}/tasks/${TASK_ID}"
    echo "[smoke] last status body:"
    curl -s "${API_BASE}/tasks/${TASK_ID}" | head -c 800; echo
    exit 2
  fi

  RESP_WITH_CODE="$(fetch "${API_BASE}/tasks/${TASK_ID}")"
  HTTP_CODE="$(echo "$RESP_WITH_CODE" | get_code)"
  BODY="$(echo "$RESP_WITH_CODE" | get_body)"

  if [[ "${HTTP_CODE}" != "200" ]]; then
    echo "[smoke] status endpoint HTTP=${HTTP_CODE} body=$(echo "$BODY" | head -c 200)"
    sleep "${SLEEP_S}"
    continue
  fi

  # Parse final status using jq (most reliable)
  FINAL_STATUS="$(echo "$BODY" | jq -r '.subtasks[]? | select(.subtask_id=="final") | .status' 2>/dev/null || true)"

  if [[ -z "${FINAL_STATUS}" || "${FINAL_STATUS}" == "null" ]]; then
    echo "[smoke] ERROR: HTTP 200 but could not parse final status from JSON."
    echo "[smoke] headers:"
    ctype "${API_BASE}/tasks/${TASK_ID}"
    echo "[smoke] body(first 400 chars):"
    echo "$BODY" | head -c 400; echo
    exit 6
  fi

  echo "[smoke] final_status=${FINAL_STATUS}"

  if [[ "${FINAL_STATUS}" == "COMPLETED" ]]; then
    break
  fi

  if [[ "${FINAL_STATUS}" == "FAILED" ]]; then
    echo "[smoke] FAILED. Dumping task status:"
    echo "$BODY" | jq . || true
    exit 3
  fi

  sleep "${SLEEP_S}"
done

echo "[smoke] fetching final"
RESP_WITH_CODE="$(fetch "${API_BASE}/tasks/${TASK_ID}/final")"
HTTP_CODE="$(echo "$RESP_WITH_CODE" | get_code)"
FINAL_BODY="$(echo "$RESP_WITH_CODE" | get_body)"

if [[ "${HTTP_CODE}" != "200" ]]; then
  echo "[smoke] final endpoint HTTP=${HTTP_CODE}"
  echo "[smoke] headers:"
  ctype "${API_BASE}/tasks/${TASK_ID}/final"
  echo "[smoke] body:"
  echo "$FINAL_BODY" | head -c 800; echo
  exit 4
fi

FINAL_LEN="$(echo "$FINAL_BODY" | jq -r '.final // "" | length' 2>/dev/null || echo 0)"
echo "[smoke] final length=${FINAL_LEN}"

if [[ "${FINAL_LEN}" -lt 20 ]]; then
  echo "[smoke] ERROR: final too short"
  echo "$FINAL_BODY" | jq . || true
  exit 5
fi

echo "[smoke] SUCCESS ✅ task_id=${TASK_ID}"