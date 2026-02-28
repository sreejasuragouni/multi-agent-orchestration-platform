import json
import os
import random
import time

from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Phase 4: orchestrator publishes all work here
IN_TOPIC  = os.environ.get("KAFKA_IN_TOPIC", "task.work")

# Worker always publishes results here; orchestrator consumes it
OUT_TOPIC = os.environ.get("KAFKA_OUT_TOPIC", "task.results")

DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "task.dlq")

ROLE = os.environ.get("AGENT_ROLE", "research")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", f"{ROLE}-agents")

# Retry is handled by orchestrator in Phase 4.
# We keep MAX_ATTEMPTS only to annotate errors or safety checks if needed.
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", "3"))

# Failure injection stays, but now worker emits FAILED and orchestrator retries.
FAIL_PROB = float(os.environ.get("FAIL_PROB", "0.0"))

consumer = KafkaConsumer(
    IN_TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print(f"[{ROLE}] group_id={GROUP_ID} listening on {IN_TOPIC}")

for msg in consumer:
    job = msg.value or {}

    # Phase 4 payload fields
    task_id = job.get("task_id")
    subtask_id = job.get("subtask_id")
    role = job.get("role")
    attempt = int(job.get("attempt", 0))
    payload = job.get("payload") or {}
    instruction = payload.get("instruction", "")
    original_task = payload.get("original_task", "")

    # Ignore jobs not meant for this worker role
    if role != ROLE:
        continue

    print(f"[{ROLE}] consumed task_id={task_id} subtask_id={subtask_id} attempt={attempt}")

    # Basic validation (if subtask_id missing, orchestrator can't update DAG)
    if not task_id or not subtask_id:
        err_msg = "missing task_id or subtask_id"
        result = {
            "task_id": task_id,
            "subtask_id": subtask_id,
            "role": ROLE,
            "status": "FAILED",
            "attempt": attempt,
            "original_task": original_task,
            "error": err_msg,
            "processed_at_ms": int(time.time() * 1000),
        }
        producer.send(OUT_TOPIC, result)
        producer.flush()
        print(f"[{ROLE}] produced FAILED (bad payload) -> {OUT_TOPIC}")
        continue

    # Simulate work
    time.sleep(1)

    # Optional failure injection to validate orchestrator retry/DLQ
    if FAIL_PROB > 0 and random.random() < FAIL_PROB:
        result = {
            "task_id": task_id,
            "subtask_id": subtask_id,
            "role": ROLE,
            "status": "FAILED",
            "attempt": attempt,
            "original_task": original_task,
            "error": f"simulated failure (FAIL_PROB={FAIL_PROB})",
            "processed_at_ms": int(time.time() * 1000),
        }
        producer.send(OUT_TOPIC, result)
        producer.flush()
        print(f"[{ROLE}] produced FAILED -> {OUT_TOPIC}")
        continue

    # Success
    result = {
        "task_id": task_id,
        "subtask_id": subtask_id,
        "role": ROLE,
        "status": "COMPLETED",
        "attempt": attempt,
        "original_task": original_task,
        "output": f"[{ROLE}] completed: {instruction}",
        "processed_at_ms": int(time.time() * 1000),
    }

    producer.send(OUT_TOPIC, result)
    producer.flush()
    print(f"[{ROLE}] produced COMPLETED task_id={task_id} subtask_id={subtask_id} -> {OUT_TOPIC}")
