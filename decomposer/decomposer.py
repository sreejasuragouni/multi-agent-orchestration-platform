import json
import os
import time
import traceback
from typing import Any, Dict, List, Optional

import requests
from db import init_schema, update_task_final, upsert_task_status
from kafka import KafkaConsumer, KafkaProducer
from ops_server import start_ops_server_in_thread
from prometheus_client import Counter
from kafka.errors import NoBrokersAvailable

# -----------------------------
# Observability (Prometheus + Ops)
# -----------------------------
SERVICE = os.environ.get("SERVICE_NAME", "decomposer")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8001"))

# replaces start_http_server(METRICS_PORT)
start_ops_server_in_thread(METRICS_PORT)

TASKS_CONSUMED = Counter("tasks_consumed_total", "Tasks consumed", ["service"])
TASKS_PRODUCED = Counter("tasks_produced_total", "Tasks produced", ["service"])
ERRORS = Counter("errors_total", "Errors", ["service", "stage"])

# -----------------------------
# Kafka
# -----------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

IN_TOPIC = os.environ.get("IN_TOPIC", "task.requests")
PLAN_TOPIC = os.environ.get("PLAN_TOPIC", "task.plan")
FINAL_TOPIC = os.environ.get("FINAL_TOPIC", "task.final")
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
# Qdrant + Ollama (Reuse)
# -----------------------------
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "tasks")
REUSE_THRESHOLD = float(os.environ.get("REUSE_THRESHOLD", "0.83"))

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434")
OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", "nomic-embed-text")


def publish_dlq(stage: str, task_id: Optional[str], task_text: str, payload: Optional[Dict[str, Any]], err: Exception):
    dlq_msg = {
        "task_id": task_id,
        "task": task_text,
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
    except Exception as e2:
        ERRORS.labels(SERVICE, "dlq_publish").inc()
        print(f"[{SERVICE}] ERROR failed to publish to DLQ: {type(e2).__name__}: {e2}", flush=True)


def ollama_embed(text: str) -> List[float]:
    r = requests.post(
        f"{OLLAMA_BASE_URL}/api/embeddings",
        json={"model": OLLAMA_EMBED_MODEL, "prompt": text},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["embedding"]


def qdrant_search(vector: List[float], limit: int = 1):
    r = requests.post(
        f"{QDRANT_URL}/collections/{COLLECTION}/points/search",
        json={"vector": vector, "limit": limit, "with_payload": True},
        timeout=20,
    )
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json().get("result", [])


def qdrant_upsert(point_id: str, vector: List[float], payload: dict):
    requests.put(
        f"{QDRANT_URL}/collections/{COLLECTION}/points?wait=true",
        json={"points": [{"id": point_id, "vector": vector, "payload": payload}]},
        timeout=20,
    ).raise_for_status()


def ensure_collection(dim: int):
    r = requests.get(f"{QDRANT_URL}/collections/{COLLECTION}", timeout=10)
    if r.status_code == 200:
        return
    payload = {"vectors": {"size": dim, "distance": "Cosine"}}
    requests.put(f"{QDRANT_URL}/collections/{COLLECTION}", json=payload, timeout=20).raise_for_status()


def fallback_plan(task_text: str):
    return [
        {"subtask_id": "s1", "role": "research", "instruction": f"Research key points about: {task_text}", "depends_on": []},
        {"subtask_id": "s2", "role": "analysis", "instruction": f"Analyze and structure an answer for: {task_text}", "depends_on": ["s1"]},
        {"subtask_id": "s3", "role": "code", "instruction": f"Provide pseudo-code or implementation hints for: {task_text}", "depends_on": ["s2"]},
    ]


# -----------------------------
# Start
# -----------------------------
init_schema()
print(f"[{SERVICE}] running (reuse enabled). threshold={REUSE_THRESHOLD} collection={COLLECTION}", flush=True)

for msg in consumer:
    TASKS_CONSUMED.labels(SERVICE).inc()

    task_id = None
    task_text = ""
    task_payload: Dict[str, Any] | None = None

    try:
        task_payload = msg.value
        task_id = task_payload.get("task_id")
        task_text = task_payload.get("task", "")

        if not task_id:
            raise ValueError("request missing task_id")

        upsert_task_status(task_id, task_text, "RECEIVED")

        vec = ollama_embed(task_text)
        hits = qdrant_search(vec, limit=1)

        if hits:
            top = hits[0]
            score = float(top.get("score", 0.0))
            print(f"[{SERVICE}] top_score={score:.6f} threshold={REUSE_THRESHOLD} top_id={top.get('id')}", flush=True)

            payload = top.get("payload") or {}
            prior_final = payload.get("final")

            if prior_final and score >= REUSE_THRESHOLD:
                reused_msg = {
                    "task_id": task_id,
                    "final": prior_final,
                    "reused": True,
                    "reused_from_task_id": str(top.get("id")),
                    "similarity_score": score,
                    "synthesized_at_ms": int(time.time() * 1000),
                }

                producer.send(FINAL_TOPIC, reused_msg)
                producer.flush()
                TASKS_PRODUCED.labels(SERVICE).inc()

                upsert_task_status(task_id, task_text, "COMPLETED")
                update_task_final(
                    task_id=task_id,
                    final_text=prior_final,
                    reused=True,
                    reused_from_task_id=str(top.get("id")),
                    similarity_score=score,
                )

                ensure_collection(len(vec))
                qdrant_upsert(
                    point_id=task_id,
                    vector=vec,
                    payload={
                        "final": prior_final,
                        "embed_text": (payload.get("embed_text") or task_text)[:6000],
                        "original_task": task_text,
                        "reused": True,
                        "reused_from_task_id": str(top.get("id")),
                        "similarity_score": score,
                        "updated_at_ms": int(time.time() * 1000),
                    },
                )

                print(f"[{SERVICE}] REUSED+STORED task_id={task_id} from={top.get('id')} score={score:.3f}", flush=True)
                continue

        plan = {
            "task_id": task_id,
            "task": task_text,
            "subtasks": fallback_plan(task_text),
            "created_at_ms": int(time.time() * 1000),
            "reused": False,
        }
        producer.send(PLAN_TOPIC, plan)
        producer.flush()
        TASKS_PRODUCED.labels(SERVICE).inc()

        upsert_task_status(task_id, task_text, "PLANNED")
        print(f"[{SERVICE}] planned task_id={task_id}", flush=True)

    except Exception as e:
        ERRORS.labels(SERVICE, "process_message").inc()
        print(f"[{SERVICE}] ERROR task_id={task_id} {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        publish_dlq(stage="process_message", task_id=task_id, task_text=task_text, payload=task_payload, err=e)
        continue