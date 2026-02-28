import json
import os
import time
import traceback
from typing import Any, Dict, List

from db import get_conn, init_schema, now_ms
from kafka import KafkaConsumer, KafkaProducer
from ops_server import start_ops_server_in_thread
from prometheus_client import Counter

# -----------------------------
# Observability (Prometheus + Ops)
# -----------------------------
SERVICE = os.environ.get("SERVICE_NAME", "orchestrator")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8002"))

# replaces start_http_server(METRICS_PORT)
start_ops_server_in_thread(METRICS_PORT)

TASKS_CONSUMED = Counter("tasks_consumed_total", "Messages consumed", ["service", "topic"])
TASKS_PRODUCED = Counter("tasks_produced_total", "Messages produced", ["service", "topic"])
ERRORS = Counter("errors_total", "Errors", ["service", "stage"])

# -----------------------------
# Kafka
# -----------------------------
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

TOPIC_PLAN = os.environ.get("KAFKA_PLAN_TOPIC", "task.plan")
TOPIC_RESULTS = os.environ.get("KAFKA_RESULTS_TOPIC", "task.results")
TOPIC_WORK = os.environ.get("KAFKA_WORK_TOPIC", "task.work")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "task.dlq")

MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    TOPIC_PLAN,
    TOPIC_RESULTS,
    bootstrap_servers=BOOTSTRAP,
    group_id="orchestrator-v2",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)


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
        print(f"[{SERVICE}] ERROR failed to publish to DLQ: {type(e2).__name__}: {e2}", flush=True)


# -----------------------------
# DB helpers
# -----------------------------
def upsert_task(task_id: str, task_text: str, status: str):
    """
    Phase 6 upgrade:
      - sets completed_at_ms when status reaches COMPLETED/FAILED
      - keeps completed_at_ms stable (won't overwrite once set)
    """
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
                subtask_id = st.get("subtask_id")
                if not subtask_id:
                    raise ValueError("subtask missing subtask_id (Phase 4 requires stable IDs)")

                cur.execute(
                    """
                    INSERT INTO subtasks(task_id, subtask_id, role, instruction, depends_on,
                                         status, attempt, created_at_ms, updated_at_ms)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, 0, %s, %s)
                    ON CONFLICT (task_id, subtask_id) DO NOTHING;
                    """,
                    (
                        task_id,
                        subtask_id,
                        st.get("role", ""),
                        st.get("instruction", ""),
                        json.dumps(st.get("depends_on", [])),
                        "PENDING",
                        ts,
                        ts,
                    ),
                )
        conn.commit()


def get_all_subtasks(task_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT subtask_id, role, instruction, depends_on, status, attempt
                FROM subtasks
                WHERE task_id=%s;
                """,
                (task_id,),
            )
            rows = cur.fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "subtask_id": r[0],
                "role": r[1],
                "instruction": r[2],
                "depends_on": r[3] or [],
                "status": r[4],
                "attempt": r[5],
            }
        )
    return out


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


def inc_attempt(task_id: str, subtask_id: str) -> int:
    ts = now_ms()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE subtasks
                SET attempt = attempt + 1, updated_at_ms=%s
                WHERE task_id=%s AND subtask_id=%s
                RETURNING attempt;
                """,
                (ts, task_id, subtask_id),
            )
            attempt = cur.fetchone()[0]
        conn.commit()
    return attempt


# -----------------------------
# Scheduling logic (DAG)
# -----------------------------
def dispatch_ready(task_id: str, original_task: str):
    subtasks = get_all_subtasks(task_id)
    statuses = {st["subtask_id"]: st["status"] for st in subtasks}

    dispatched = 0
    for st in subtasks:
        if st["status"] != "PENDING":
            continue

        deps = st["depends_on"] or []
        if all(statuses.get(d) == "COMPLETED" for d in deps):
            mark_subtask(task_id, st["subtask_id"], "DISPATCHED")

            payload = {
                "task_id": task_id,
                "subtask_id": st["subtask_id"],
                "role": st["role"],
                "attempt": st["attempt"],
                "created_at_ms": int(time.time() * 1000),
                "payload": {
                    "instruction": st["instruction"],
                    "original_task": original_task,
                },
            }

            producer.send(TOPIC_WORK, payload)
            TASKS_PRODUCED.labels(SERVICE, TOPIC_WORK).inc()
            dispatched += 1

    if dispatched:
        producer.flush()


def all_done(task_id: str) -> bool:
    subtasks = get_all_subtasks(task_id)
    return len(subtasks) > 0 and all(st["status"] == "COMPLETED" for st in subtasks)


# -----------------------------
# Handlers
# -----------------------------
def handle_plan(plan: Dict[str, Any]):
    task_id = plan.get("task_id")
    original_task = plan.get("task", "")
    subtasks = plan.get("subtasks", []) or []

    if not task_id:
        raise ValueError("plan missing task_id")

    upsert_task(task_id, original_task, "RUNNING")
    insert_subtasks(task_id, subtasks)

    dispatch_ready(task_id, original_task)


def handle_result(res: Dict[str, Any]):
    task_id = res.get("task_id")
    subtask_id = res.get("subtask_id")
    status_str = res.get("status")

    original_task = res.get("original_task") or (res.get("payload") or {}).get("original_task") or ""

    if not task_id or not subtask_id:
        raise ValueError("result missing task_id or subtask_id")

    if status_str == "COMPLETED":
        mark_subtask(task_id, subtask_id, "COMPLETED", None)
        dispatch_ready(task_id, original_task)

        if all_done(task_id):
            upsert_task(task_id, original_task, "COMPLETED")
        return

    # FAILED / ERROR path
    err = res.get("error", "unknown error")
    attempt = inc_attempt(task_id, subtask_id)

    if attempt < MAX_ATTEMPTS:
        mark_subtask(task_id, subtask_id, "PENDING", err)
        dispatch_ready(task_id, original_task)
    else:
        mark_subtask(task_id, subtask_id, "FAILED", err)
        upsert_task(task_id, original_task, "FAILED")
        producer.send(DLQ_TOPIC, res)
        producer.flush()
        TASKS_PRODUCED.labels(SERVICE, DLQ_TOPIC).inc()


# -----------------------------
# Start
# -----------------------------
init_schema()
print(
    f"[{SERVICE}] consuming from {TOPIC_PLAN} and {TOPIC_RESULTS} | producing to {TOPIC_WORK}",
    flush=True,
)

for msg in consumer:
    TASKS_CONSUMED.labels(SERVICE, msg.topic).inc()
    payload = None
    try:
        payload = msg.value
        if msg.topic == TOPIC_PLAN:
            handle_plan(payload)
        elif msg.topic == TOPIC_RESULTS:
            handle_result(payload)
    except Exception as e:
        ERRORS.labels(SERVICE, "process_message").inc()
        print(f"[{SERVICE}] ERROR {type(e).__name__}: {e}", flush=True)
        traceback.print_exc()
        publish_dlq(stage="process_message", task_id=(payload or {}).get("task_id"), payload=payload, err=e)