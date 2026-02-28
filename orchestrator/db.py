import os
import time
from typing import Any, Dict, Optional

import psycopg2


def now_ms() -> int:
    return int(time.time() * 1000)


def get_conn():
    """
    Orchestrator uses PG_DSN from docker-compose:
      PG_DSN=postgresql://app:app@postgres:5432/orchestrator
    """
    dsn = os.environ.get("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN is not set for this service")
    return psycopg2.connect(dsn)


def init_schema():
    """
    Creates/updates core tables used by orchestrator Phase-4/6:
      - tasks
      - subtasks
      - task_final (used in Phase-6 when API wants /final)
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
                  completed_at_ms BIGINT NULL
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS subtasks (
                  task_id TEXT NOT NULL,
                  subtask_id TEXT NOT NULL,
                  role TEXT NOT NULL,
                  instruction TEXT NOT NULL,
                  depends_on JSONB NOT NULL DEFAULT '[]'::jsonb,
                  status TEXT NOT NULL,
                  attempt INT NOT NULL DEFAULT 0,
                  last_error TEXT NULL,
                  created_at_ms BIGINT NOT NULL,
                  updated_at_ms BIGINT NOT NULL,
                  PRIMARY KEY (task_id, subtask_id)
                );
                """
            )

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS task_final (
                  task_id TEXT PRIMARY KEY,
                  final TEXT NOT NULL,
                  reused BOOLEAN NOT NULL DEFAULT FALSE,
                  reused_from_task_id TEXT NULL,
                  similarity_score DOUBLE PRECISION NULL,
                  synthesized_at_ms BIGINT NULL,
                  updated_at_ms BIGINT NOT NULL
                );
                """
            )
        conn.commit()


# -----------------------
# Phase-6 helpers: finals
# -----------------------
def upsert_final(
    task_id: str,
    final: str,
    reused: bool = False,
    reused_from_task_id: Optional[str] = None,
    similarity_score: Optional[float] = None,
    synthesized_at_ms: Optional[int] = None,
):
    ts = now_ms()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_final (
                  task_id, final, reused, reused_from_task_id, similarity_score,
                  synthesized_at_ms, updated_at_ms
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (task_id) DO UPDATE
                  SET final = EXCLUDED.final,
                      reused = EXCLUDED.reused,
                      reused_from_task_id = EXCLUDED.reused_from_task_id,
                      similarity_score = EXCLUDED.similarity_score,
                      synthesized_at_ms = EXCLUDED.synthesized_at_ms,
                      updated_at_ms = EXCLUDED.updated_at_ms;
                """,
                (
                    task_id,
                    final,
                    reused,
                    reused_from_task_id,
                    similarity_score,
                    synthesized_at_ms,
                    ts,
                ),
            )
        conn.commit()


def get_final(task_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT task_id, final, reused, reused_from_task_id, similarity_score,
                       synthesized_at_ms, updated_at_ms
                FROM task_final
                WHERE task_id = %s;
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
