# decomposer/db.py
import os
import time
from contextlib import contextmanager
from typing import Optional

import psycopg2


def now_ms() -> int:
    return int(time.time() * 1000)


def _build_dsn() -> str:
    # Prefer PG_DSN if present (some services use it)
    dsn = os.environ.get("PG_DSN")
    if dsn:
        return dsn

    host = os.environ.get("DB_HOST", "postgres")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "orchestrator")
    user = os.environ.get("DB_USER", "app")
    password = os.environ.get("DB_PASSWORD", "app")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


@contextmanager
def get_conn():
    dsn = _build_dsn()
    conn = psycopg2.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()


def init_schema():
    """
    Create the minimal tables decomposer writes to:
      - tasks
      - task_final
    We keep this compatible with your API queries.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    task_text TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at_ms BIGINT NOT NULL,
                    updated_at_ms BIGINT NOT NULL,
                    completed_at_ms BIGINT
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_final (
                    task_id TEXT PRIMARY KEY,
                    final_text TEXT NOT NULL,
                    reused BOOLEAN NOT NULL DEFAULT FALSE,
                    reused_from_task_id TEXT,
                    similarity_score DOUBLE PRECISION,
                    synthesized_at_ms BIGINT,
                    updated_at_ms BIGINT NOT NULL
                );
                """
            )

        conn.commit()


def upsert_task_status(task_id: str, task_text: str, status: str):
    ts = now_ms()
    completed_at = ts if status in ("COMPLETED", "FAILED") else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks(task_id, task_text, status, created_at_ms, updated_at_ms, completed_at_ms)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE
                  SET task_text=EXCLUDED.task_text,
                      status=EXCLUDED.status,
                      updated_at_ms=EXCLUDED.updated_at_ms,
                      completed_at_ms=COALESCE(tasks.completed_at_ms, EXCLUDED.completed_at_ms);
                """,
                (task_id, task_text, status, ts, ts, completed_at),
            )
        conn.commit()


def update_task_final(
    task_id: str,
    final_text: str,
    reused: bool,
    reused_from_task_id: Optional[str],
    similarity_score: Optional[float],
):
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
                (task_id, final_text, reused, reused_from_task_id, similarity_score, ts, ts),
            )
        conn.commit()