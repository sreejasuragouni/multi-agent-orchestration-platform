# orchestrator/orchestrator.py

import json
import os
import time
import traceback
from typing import Any, Dict, List

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import Counter

from db import get_conn, init_schema, now_ms
from ops_server import start_ops_server_in_thread

# -----------------------------
# Observability (Prometheus + Ops)
# -----------------------------
SERVICE = os.environ.get("SERVICE_NAME", "orchestrator")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8002"))
start_ops_server_in_thread(METRICS_PORT)

TASKS_CONSUMED = Counter("tasks_consumed_total", "Messages consumed", ["service", "topic"])
TASKS_PRODUCED = Counter("tasks_produced_total", "Messages produced", ["service", "topic"])
ERRORS = Counter("errors_total", "Errors", ["service", "stage"])

# -----------------------------
# Kafka config
# -----------------------------
BOOTSTRAP = (
    os.getenv("KAFKA_BOOTSTRAP_SERVERS")   # ✅ what compose sets
    or os.getenv("BOOTSTRAP_SERVERS")      # fallback if you ever rename
    or "kafka:9092"                        # safe default inside compose
)

TOPIC_REQUESTS = os.environ.get("KAFKA_REQUEST_TOPIC", "task.requests")
TOPIC_PLAN = os.environ.get("KAFKA_PLAN_TOPIC", "task.plan")
TOPIC_RESULTS = os.environ.get("KAFKA_RESULTS_TOPIC", "task.results")
TOPIC_WORK = os.environ.get("KAFKA_WORK_TOPIC", "task.work")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", os.environ.get("DLQ_TOPIC", "task.dlq"))

MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

# Option B mode: no decomposer/worker/synth required.
# Keep this "true" for CI smoke stability.
CI_STUB_MODE = os.environ.get("CI_STUB_MODE", "true").lower() in ("1", "true", "yes")

# -----------------------------
# Producer (retry)
# -----------------------------
def create_producer() -> KafkaProducer:
    last_err = None
    for _ in range(60):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=10000,
                request_timeout_ms=10000,
                retries=5,
            )
        except NoBrokersAvailable as e:
            last_err = e
            time.sleep(1)
    raise last_err or NoBrokersAvailable()

producer = create_producer()

# -----------------------------
# Consumer (retry)  ✅ this is what you were missing
# -----------------------------
def create_consumer() -> KafkaConsumer:
    last_err = None
    topics = [TOPIC_REQUESTS] if CI_STUB_MODE else [TOPIC_REQUESTS, TOPIC_PLAN, TOPIC_RESULTS]

    # ✅ must satisfy: request_timeout_ms > session_timeout_ms
    session_timeout_ms = 10000
    request_timeout_ms = 30000

    for _ in range(60):
        try:
            return KafkaConsumer(
                *topics,
                bootstrap_servers=BOOTSTRAP,
                group_id=os.environ.get("KAFKA_GROUP_ID", "orchestrator-ci"),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),

                # timeouts
                session_timeout_ms=session_timeout_ms,
                request_timeout_ms=request_timeout_ms,
                api_version_auto_timeout_ms=10000,
            )
        except NoBrokersAvailable as e:
            last_err = e
            time.sleep(1)

    raise last_err or NoBrokersAvailable()

# -----------------------------
# DLQ
# -----------------------------
def publish_dlq(stage: str, task_id: str | None, payload: Dict[str, Any] | None, err: Exception):
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
        print(f"[{SERVICE}] ERROR DLQ publish failed: {type(e2).__name__}: {e2}", flush=True)

# -----------------------------
# DB helpers
# -----------------------------
def upsert_task(task_id: str, task_text: str, status: str):
    ts = now_ms()
    completed_at = ts if status in ("COMPLETED", "FAILED") else None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks(task_id, task_text, status, created_at_ms, updated_at_ms, completed_at_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE
                  SET status=EXCLUDED.status,
                      task_text=EXCLUDED.task_text,
                      updated_at_ms=EXCLUDED.updated_at_ms,
                      completed_at_ms=COALESCE(tasks.completed_at_ms, EXCLUDED.completed_at_ms);
                """,
                (task_id, task_text, status, ts, ts, completed_at),
            )
        conn.commit()

def insert_subtasks(task_id: str, subtasks: List[Dict[str, Any]]):
    ts = now_ms()
    with get_conn() as conn:
        with conn.cursor() as cur:
            for st in subtasks:
                cur.execute(
                    """
                    INSERT INTO subtasks(task_id, subtask_id, role, instruction, depends_on,
                                         status, attempt, created_at_ms, updated_at_ms)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, 0, %s, %s)
                    ON CONFLICT (task_id, subtask_id) DO NOTHING;
                    """,
                    (
                        task_id,
                        st["subtask_id"],
                        st.get("role", ""),
                        st.get("instruction", ""),
                        json.dumps(st.get("depends_on", [])),
                        "PENDING",
                        ts,
                        ts,
                    ),
                )
        conn.commit()

def mark_subtask(task_id: str, subtask_id: str, status: str, err: str | None = None):
    ts = now_ms()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE subtasks
                SET status=%s, last_error=%s, updated_at_ms=%s
                WHERE task_id=%s AND subtask_id=%s;
                """,
                (status, err, ts, task_id, subtask_id),
            )
        conn.commit()

# -----------------------------
# Option B handler (CI stub)
# -----------------------------
def handle_request(payload: dict) -> None:
    task_id = payload["task_id"]
    task_text = payload.get("task", "")

    print(f"[{SERVICE}] handle_request task_id={task_id}", flush=True)

    upsert_task(task_id, task_text, "RUNNING")

    insert_subtasks(task_id, [
        {"subtask_id": "s1", "role": "research", "instruction": f"Research: {task_text}", "depends_on": []},
        {"subtask_id": "s2", "role": "analysis",  "instruction": f"Analyze: {task_text}",  "depends_on": ["s1"]},
        {"subtask_id": "s3", "role": "code",      "instruction": f"Code: {task_text}",     "depends_on": ["s2"]},
    ])

    mark_subtask(task_id, "s1", "COMPLETED")
    mark_subtask(task_id, "s2", "COMPLETED")
    mark_subtask(task_id, "s3", "COMPLETED")

    final_text = (
        "Final summary (CI stub)\n\n"
        f"Task: {task_text}\n\n"
        "- Research: Completed (stub)\n"
        "- Analysis: Completed (stub)\n"
        "- Code: Completed (stub)\n\n"
        "Result: Pipeline completed without workers for CI smoke validation."
    )

    ts = now_ms()
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
                (task_id, final_text, False, None, None, ts, ts),
            )
        conn.commit()

    upsert_task(task_id, task_text, "COMPLETED")

# -----------------------------
# Start
# -----------------------------
init_schema()

print(
    f"[{SERVICE}] CI_STUB_MODE={CI_STUB_MODE} consuming from {TOPIC_REQUESTS}" +
    ("" if CI_STUB_MODE else f", {TOPIC_PLAN}, {TOPIC_RESULTS}"),
    flush=True,
)

consumer = create_consumer()   # ✅ THIS LINE MUST EXIST (global var)

for msg in consumer:           # ✅ now consumer is defined
    print(f"[{SERVICE}] GOT msg topic={msg.topic} offset={msg.offset}", flush=True)
    TASKS_CONSUMED.labels(SERVICE, msg.topic).inc()
    payload = None
    try:
        payload = msg.value

        if msg.topic == TOPIC_REQUESTS:
            handle_request(payload)
        elif (not CI_STUB_MODE) and msg.topic == TOPIC_PLAN:
            handle_plan(payload)
        elif (not CI_STUB_MODE) and msg.topic == TOPIC_RESULTS:
            handle_result(payload)

    except Exception as e:
        ERRORS.labels(SERVICE, "process_message").inc()
        print(f"[{SERVICE}] ERROR {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        publish_dlq(
            stage="process_message",
            task_id=(payload or {}).get("task_id"),
            payload=payload,
            err=e,
        )