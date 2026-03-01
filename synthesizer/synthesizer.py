import json
import os
import time
import traceback
from collections import defaultdict
from typing import Any, Dict, Optional

import requests
from db import get_conn
from kafka import KafkaConsumer, KafkaProducer
from ops_server import start_ops_server_in_thread
from prometheus_client import Counter, Histogram
from kafka.errors import NoBrokersAvailable

# -----------------------------
# Observability (Prometheus + Ops)
# -----------------------------
SERVICE = os.environ.get("SERVICE_NAME", "synthesizer")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8003"))

# replaces start_http_server(METRICS_PORT)
start_ops_server_in_thread(METRICS_PORT)

TASKS_CONSUMED = Counter("tasks_consumed_total", "Messages consumed", ["service", "topic"])
TASKS_PRODUCED = Counter("tasks_produced_total", "Messages produced", ["service", "topic"])
ERRORS = Counter("errors_total", "Errors", ["service", "stage"])

# Phase 7 metrics (requested)
FINAL_WRITTEN = Counter("final_written_total", "Final rows written to Postgres", ["service"])
FINAL_WRITE_FAIL = Counter("final_write_fail_total", "Failures writing final to Postgres", ["service"])
FINAL_LATENCY_MS = Histogram(
    "final_latency_ms",
    "Time from first buffered subtask result to final publish",
    ["service"],
    buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10000, 20000, 60000),
)


# -----------------------------
# Kafka
# -----------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
IN_TOPIC = os.environ.get("KAFKA_IN_TOPIC", "task.results")
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", "task.final")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "task.dlq")

consumer = None
last_err = None

for _ in range(60):  # ~60s
    try:
        consumer = KafkaConsumer(
            TOPIC_REQUESTS,                  # whatever topics decomposer consumes
            bootstrap_servers=BOOTSTRAP,
            group_id="decomposer-v1",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            api_version_auto_timeout_ms=5000,
            request_timeout_ms=5000,
        )
        break
    except NoBrokersAvailable as e:
        last_err = e
        time.sleep(1)

if consumer is None:
    raise last_err or NoBrokersAvailable()

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


# -----------------------------
# Qdrant + Ollama
# -----------------------------
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "tasks")

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")

EXPECTED_ROLES = {"research", "analysis", "code"}

# Buffer: task_id -> role -> normalized result
buf: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)

# Track when we first saw any COMPLETED result for a task_id (for latency)
first_seen_ms: Dict[str, int] = {}


def publish_dlq(stage: str, task_id: Optional[str], payload: Optional[Dict[str, Any]], err: Exception):
    dlq_msg = {
        "task_id": task_id,
        "service": SERVICE,
        "stage": stage,
        "error_type": type(err).__name__,
        "error": str(err),
        "payload": payload or {},
        "failed_at_ms": int(time.time() * 1000),
    }
    try:
        producer.send(DLQ_TOPIC, dlq_msg)
        producer.flush()
        TASKS_PRODUCED.labels(SERVICE, DLQ_TOPIC).inc()
    except Exception as e2:
        ERRORS.labels(SERVICE, "dlq_publish").inc()
        print(f"[{SERVICE}] ERROR failed to publish to DLQ: {type(e2).__name__}: {e2}", flush=True)


def ollama_embed(text: str) -> list[float]:
    r = requests.post(
        f"{OLLAMA_BASE_URL}/api/embeddings",
        json={"model": OLLAMA_EMBED_MODEL, "prompt": text},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["embedding"]


def ensure_collection(dim: int):
    r = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10)
    if r.status_code == 200:
        return
    payload = {"vectors": {"size": dim, "distance": "Cosine"}}
    requests.put(f"{QDRANT_URL}/collections/{COLLECTION}", json=payload, timeout=20).raise_for_status()


def qdrant_upsert(point_id: str, vector: list[float], payload: dict):
    requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points?wait=true",
        json={"points": [{"id": point_id, "vector": vector, "payload": payload}]},
        timeout=20,
    ).raise_for_status()


def normalize_result(res: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Accept both schemas:
      Phase 2: {task_id, agent_role, result, ...}
      Phase 4/5: {task_id, subtask_id, role, status, output, original_task, ...}
    """
    task_id = res.get("task_id")
    role = res.get("role") or res.get("agent_role")
    if not task_id or not role:
        return None

    status = res.get("status") or "COMPLETED"
    output = res.get("output")
    if output is None:
        output = res.get("result", "")

    original_task = res.get("original_task")
    if original_task is None:
        original_task = (res.get("payload") or {}).get("original_task", "")

    return {
        "task_id": task_id,
        "role": role,
        "status": status,
        "output": output or "",
        "original_task": original_task or "",
        "subtask_id": res.get("subtask_id"),
        "attempt": res.get("attempt", 0),
        "processed_at_ms": res.get("processed_at_ms"),
    }


def ensure_db_schema():
    """
    Ensure task_final exists with the production schema.
    We standardize on final_text only (no 'final' column).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_final (
                    task_id TEXT PRIMARY KEY,
                    final_text TEXT NOT NULL,
                    reused BOOLEAN DEFAULT FALSE,
                    reused_from_task_id TEXT,
                    similarity_score DOUBLE PRECISION,
                    synthesized_at_ms BIGINT,
                    updated_at_ms BIGINT
                );
                """
            )
        conn.commit()


def upsert_final_text(
    task_id: str,
    final_text: str,
    reused: bool,
    reused_from_task_id: Optional[str],
    similarity_score: Optional[float],
    synthesized_at_ms: int,
):
    now = int(time.time() * 1000)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_final(
                    task_id, final_text, reused, reused_from_task_id,
                    similarity_score, synthesized_at_ms, updated_at_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE
                  SET final_text=EXCLUDED.final_text,
                      reused=EXCLUDED.reused,
                      reused_from_task_id=EXCLUDED.reused_from_task_id,
                      similarity_score=EXCLUDED.similarity_score,
                      synthesized_at_ms=EXCLUDED.synthesized_at_ms,
                      updated_at_ms=EXCLUDED.updated_at_ms;
                """,
                (
                    task_id,
                    final_text,
                    reused,
                    reused_from_task_id,
                    similarity_score,
                    synthesized_at_ms,
                    now,
                ),
            )
        conn.commit()


# -----------------------------
# Start
# -----------------------------
ensure_db_schema()
print(f"[{SERVICE}] consuming {IN_TOPIC} -> producing {OUT_TOPIC}", flush=True)

for msg in consumer:
    TASKS_CONSUMED.labels(SERVICE, msg.topic).inc()

    raw: Optional[Dict[str, Any]] = None
    task_id: Optional[str] = None

    try:
        raw = msg.value
        norm = normalize_result(raw)
        if not norm:
            ERRORS.labels(SERVICE, "invalid_message").inc()
            continue

        task_id = norm["task_id"]
        role = norm["role"]

        if role not in EXPECTED_ROLES:
            continue

        # Track first completion time for latency
        if task_id not in first_seen_ms:
            first_seen_ms[task_id] = int(time.time() * 1000)

        if norm["status"] != "COMPLETED":
            ERRORS.labels(SERVICE, "subtask_failed").inc()
            producer.send(DLQ_TOPIC, {"task_id": task_id, "failed_result": norm})
            producer.flush()
            TASKS_PRODUCED.labels(SERVICE, DLQ_TOPIC).inc()
            if task_id in buf:
                del buf[task_id]
            if task_id in first_seen_ms:
                del first_seen_ms[task_id]
            continue

        buf[task_id][role] = norm

        if not EXPECTED_ROLES.issubset(set(buf[task_id].keys())):
            continue

        research = buf[task_id]["research"]["output"]
        analysis = buf[task_id]["analysis"]["output"]
        code = buf[task_id]["code"]["output"]
        original_task = (
            buf[task_id]["analysis"].get("original_task")
            or buf[task_id]["research"].get("original_task")
            or buf[task_id]["code"].get("original_task")
            or ""
        )

        embed_text = (analysis + "\n" + research)[:6000]

        final_text = (
            f"FINAL ANSWER (task_id={task_id})\n\n"
            f"Task:\n{original_task}\n\n"
            f"Research:\n{research}\n\n"
            f"Analysis:\n{analysis}\n\n"
            f"Code:\n{code}\n"
        )

        synthesized_at_ms = int(time.time() * 1000)

        # --- store final in Postgres for API retrieval (single source of truth) ---
        try:
            upsert_final_text(
                task_id=task_id,
                final_text=final_text,
                reused=False,
                reused_from_task_id=None,
                similarity_score=None,
                synthesized_at_ms=synthesized_at_ms,
            )
            FINAL_WRITTEN.labels(SERVICE).inc()
        except Exception as e:
            FINAL_WRITE_FAIL.labels(SERVICE).inc()
            ERRORS.labels(SERVICE, "final_db_write").inc()
            raise e

        # publish final to Kafka
        final_msg = {
            "task_id": task_id,
            "final": final_text,
            "synthesized_at_ms": synthesized_at_ms,
            "reused": False,
        }
        producer.send(OUT_TOPIC, final_msg)
        producer.flush()
        TASKS_PRODUCED.labels(SERVICE, OUT_TOPIC).inc()

        # store in Qdrant
        vec = ollama_embed(embed_text)
        ensure_collection(len(vec))

        qdrant_upsert(
            point_id=task_id,
            vector=vec,
            payload={
                "final": final_text,
                "embed_text": embed_text,
                "original_task": original_task,
                "reused": False,
                "updated_at_ms": int(time.time() * 1000),
            },
        )

        # record end-to-end finalization latency (from first_seen to now)
        start_ms = first_seen_ms.get(task_id, synthesized_at_ms)
        FINAL_LATENCY_MS.labels(SERVICE).observe(max(0, synthesized_at_ms - start_ms))

        print(f"[{SERVICE}] finalized+stored task_id={task_id} dim={len(vec)}", flush=True)
        del buf[task_id]
        if task_id in first_seen_ms:
            del first_seen_ms[task_id]

    except Exception as e:
        ERRORS.labels(SERVICE, "process_message").inc()
        print(f"[{SERVICE}] ERROR task_id={task_id} {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        publish_dlq(stage="process_message", task_id=task_id, payload=raw, err=e)

        if task_id and task_id in buf:
            try:
                del buf[task_id]
            except Exception:
                pass
        if task_id and task_id in first_seen_ms:
            try:
                del first_seen_ms[task_id]
            except Exception:
                pass