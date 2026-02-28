import os
import time
from typing import Optional

import psycopg2


def now_ms() -> int:
    return int(time.time() * 1000)


def get_conn():
    """
    Provide DB connection to synth.
    Recommended env: PG_DSN (like orchestrator) OR DB_* vars.
    We support both so it's easy.
    """
    dsn = os.environ.get("PG_DSN")
    if dsn:
        return psycopg2.connect(dsn)

    host = os.environ.get("DB_HOST", "postgres")
    port = int(os.environ.get("DB_PORT", "5432"))
    name = os.environ.get("DB_NAME", "orchestrator")
    user = os.environ.get("DB_USER", "app")
    password = os.environ.get("DB_PASSWORD", "app")
    return psycopg2.connect(host=host, port=port, dbname=name, user=user, password=password)


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
