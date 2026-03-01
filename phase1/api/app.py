# api/app.py
import json
import os
import time
import uuid
from typing import Optional

from db import get_conn, init_schema
from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REQUEST_TOPIC = os.environ.get("KAFKA_REQUEST_TOPIC", "task.requests")

app = FastAPI(title="Task Submission API (Phase 6)")
#init_schema()  # Ensure DB schema is initialized on startup

@app.on_event("startup")
def _startup():
    # retry a bit so container startup order doesn't kill api
    last = None
    for _ in range(30):
        try:
            init_schema()
            return
        except Exception as e:
            last = e
            time.sleep(1)
    raise last

class TaskRequest(BaseModel):
    task: str

_producer: Optional[KafkaProducer] = None

def get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer

    last_err = None
    for _ in range(30):
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version_auto_timeout_ms=5000,
                request_timeout_ms=5000,
            )
            return _producer
        except NoBrokersAvailable as e:
            last_err = e
            time.sleep(1)

    raise last_err or NoBrokersAvailable()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/tasks")
def submit_task(req: TaskRequest):
    task_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)

    # 1) Persist task immediately so GET /tasks/{task_id} works
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (task_id, task_text, status, created_at_ms, updated_at_ms)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (task_id, req.task, "QUEUED", now_ms, now_ms),
                )
            conn.commit()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to persist task: {e}")

    payload = {
        "task_id": task_id,
        "task": req.task,
        "created_at_ms": now_ms,
    }

    # 2) Publish to Kafka
    try:
        producer = get_producer()
        producer.send(REQUEST_TOPIC, payload)
        producer.flush()
    except NoBrokersAvailable:
        # Optionally mark as FAILED in DB since we already inserted
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET status=%s, updated_at_ms=%s WHERE task_id=%s",
                    ("FAILED", int(time.time() * 1000), task_id),
                )
            conn.commit()
        raise HTTPException(status_code=503, detail="Kafka not available yet. Try again.")
    except Exception as e:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE tasks SET status=%s, updated_at_ms=%s WHERE task_id=%s",
                    ("FAILED", int(time.time() * 1000), task_id),
                )
            conn.commit()
        raise HTTPException(status_code=500, detail=f"Failed to publish task: {e}")

    return {"status": "queued", "task_id": task_id, "topic": REQUEST_TOPIC}


@app.get("/tasks/{task_id}")
def get_task_status(task_id: str):
    # task row
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT task_id, task_text, status, created_at_ms, updated_at_ms, completed_at_ms
                FROM tasks
                WHERE task_id=%s
                """,
                (task_id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="task_id not found")

            task = {
                "task_id": row[0],
                "task": row[1],
                "status": row[2],
                "created_at_ms": row[3],
                "updated_at_ms": row[4],
                "completed_at_ms": row[5],
            }

            # subtasks
            cur.execute(
                """
                SELECT subtask_id, role, instruction, depends_on, status, attempt, last_error, created_at_ms, updated_at_ms
                FROM subtasks
                WHERE task_id=%s
                ORDER BY subtask_id
                """,
                (task_id,),
            )
            subs = []
            for r in cur.fetchall():
                subs.append({
                    "subtask_id": r[0],
                    "role": r[1],
                    "instruction": r[2],
                    "depends_on": r[3] or [],
                    "status": r[4],
                    "attempt": r[5],
                    "last_error": r[6],
                    "created_at_ms": r[7],
                    "updated_at_ms": r[8],
                })
            # ---- Add a synthetic "final" stage based on task_final presence ----
            cur.execute("SELECT updated_at_ms FROM task_final WHERE task_id=%s", (task_id,))
            r_final = cur.fetchone()
            has_final = r_final is not None

            if has_final:
                final_status = "COMPLETED"
                final_updated_at = r_final[0] or task["updated_at_ms"]
            else:
                final_status = "FAILED" if task["status"] == "COMPLETED" else "PENDING"
                final_updated_at = task["updated_at_ms"]

            subs.append({
                "subtask_id": "final",
                "role": "final",
                "instruction": "Synthesize final response (from research+analysis+code)",
                "depends_on": ["s3"],
                "status": final_status,
                "attempt": 0,
                "last_error": None if has_final else ("final row not found in task_final" if final_status == "FAILED" else None),
                "created_at_ms": task["created_at_ms"],
                "updated_at_ms": final_updated_at,
            })


    task["subtasks"] = subs
    return task

@app.get("/tasks/{task_id}/final")
def get_task_final(task_id: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT task_id, final_text, reused, reused_from_task_id,
                        similarity_score, synthesized_at_ms, updated_at_ms
                    FROM task_final
                    WHERE task_id=%s
                    """,
                    (task_id,),
                
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(
                    status_code=404,
                    detail="final not found for task_id (not completed yet?)",
                )

    return {
        "task_id": row[0],
        "final": row[1],
        "reused": row[2],
        "reused_from_task_id": row[3],
        "similarity_score": row[4],
        "synthesized_at_ms": row[5],
        "updated_at_ms": row[6],
    }
@app.get("/ready")
def ready():
    # Postgres check
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
                cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"postgres not ready: {e}")

    # Kafka check (force a metadata round-trip; don't rely on cached producer state)
    try:
        from kafka import KafkaConsumer

        c = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000,
            consumer_timeout_ms=2000,
        )
        c.close()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"kafka not ready: {e}")

    # Qdrant check (optional)
    try:
        import requests
        qdrant_url = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/")
        r = requests.get(f"{qdrant_url}/healthz", timeout=2)
        if r.status_code >= 400:
            raise RuntimeError(f"qdrant status {r.status_code}")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"qdrant not ready: {e}")

    return {"ready": True}