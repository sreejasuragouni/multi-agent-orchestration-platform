# orchestrator/ops_server.py
import os
import threading
import time

from db import get_conn
from fastapi import FastAPI, Response, status
from kafka import KafkaConsumer
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

START_TS = time.time()


def _check_postgres():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()


def _check_kafka():
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    c = KafkaConsumer(
        bootstrap_servers=[s.strip() for s in bootstrap.split(",") if s.strip()],
        request_timeout_ms=2000,
        api_version_auto_timeout_ms=2000,
        consumer_timeout_ms=2000,
    )
    c.close()


def make_app() -> FastAPI:
    app = FastAPI()

    @app.get("/metrics")
    def metrics():
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/health")
    def health():
        return {"status": "ok", "uptime_s": int(time.time() - START_TS)}

    @app.get("/ready")
    def ready(resp: Response):
        failures = []

        try:
            _check_postgres()
        except Exception as e:
            failures.append({"dep": "postgres", "error": str(e)})

        try:
            _check_kafka()
        except Exception as e:
            failures.append({"dep": "kafka", "error": str(e)})

        if failures:
            resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"ready": False, "failures": failures}

        return {"ready": True}

    return app


def _run(port: int):
    import uvicorn  # local import so module import doesn't fail if uvicorn missing

    uvicorn.run(make_app(), host="0.0.0.0", port=port, log_level="info")


def start_ops_server_in_thread(port: int):
    t = threading.Thread(target=_run, args=(port,), daemon=True)
    t.start()
    return t