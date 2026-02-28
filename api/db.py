import os
import time
from typing import Any, Dict, Optional

import psycopg2


def now_ms() -> int:
    return int(time.time() * 1000)


def get_conn():
    """
    API uses DB_* env vars from docker-compose:
      DB_HOST=postgres, DB_PORT=5432, DB_NAME=orchestrator, DB_USER=app, DB_PASSWORD=app
    """
    host = os.environ.get("DB_HOST", "postgres")
    port = int(os.environ.get("DB_PORT", "5432"))
    name = os.environ.get("DB_NAME", "orchestrator")
    user = os.environ.get("DB_USER", "app")
    password = os.environ.get("DB_PASSWORD", "app")
    return psycopg2.connect(host=host, port=port, dbname=name, user=user, password=password)


def init_schema() -> None:
    """
    Make API resilient: if Postgres volume was wiped, recreate required tables.
    This keeps /tasks/{id} from 500'ing due to missing relations.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at_ms BIGINT,
                    updated_at_ms BIGINT,
                    completed_at_ms BIGINT
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS subtasks (
                    task_id TEXT NOT NULL,
                    subtask_id TEXT NOT NULL,
                    role TEXT,
                    instruction TEXT,
                    depends_on JSONB,
                    status TEXT,
                    attempt INT DEFAULT 0,
                    last_error TEXT,
                    created_at_ms BIGINT,
                    updated_at_ms BIGINT,
                    PRIMARY KEY (task_id, subtask_id)
                );
                """
            )

            # Phase 7 standard: final_text column
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


def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT task_id, task_text, status, created_at_ms, updated_at_ms, completed_at_ms
                FROM tasks WHERE task_id=%s;
                """,
                (task_id,),
            )
            t = cur.fetchone()
            if not t:
                return None

            cur.execute(
                """
                SELECT subtask_id, role, instruction, depends_on, status, attempt, last_error,
                       created_at_ms, updated_at_ms
                FROM subtasks
                WHERE task_id=%s
                ORDER BY created_at_ms ASC;
                """,
                (task_id,),
            )
            subs = cur.fetchall()

    subtasks = []
    for r in subs:
        subtasks.append(
            {
                "subtask_id": r[0],
                "role": r[1],
                "instruction": r[2],
                "depends_on": r[3] or [],
                "status": r[4],
                "attempt": r[5],
                "last_error": r[6],
                "created_at_ms": r[7],
                "updated_at_ms": r[8],
            }
        )

    return {
        "task_id": t[0],
        "task": t[1],
        "status": t[2],
        "created_at_ms": t[3],
        "updated_at_ms": t[4],
        "completed_at_ms": t[5],
        "subtasks": subtasks,
    }


def get_final(task_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT task_id, final_text, reused, reused_from_task_id, similarity_score,
                       synthesized_at_ms, updated_at_ms
                FROM task_final
                WHERE task_id=%s;
                """,
                (task_id,),
            )
            row = cur.fetchone()

    if not row:
        return None

    return {
        "task_id": row[0],
        "final": row[1],
        "reused": row[2],
        "reused_from_task_id": row[3],
        "similarity_score": row[4],
        "synthesized_at_ms": row[5],
        "updated_at_ms": row[6],
    }