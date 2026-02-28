# synthesizer/ops_server.py
import os
import threading
import time
from urllib.parse import urljoin

import requests
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


def _check_qdrant():
    qdrant_url = os.environ.get("QDRANT_URL", "http://qdrant:6333").rstrip("/") + "/"
    url = urljoin(qdrant_url, "healthz")
    r = requests.get(url, timeout=2)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}")


def _check_ollama():
    base = os.environ.get("OLLAMA_BASE_URL", "http://host.docker.internal:11434").rstrip("/") + "/"
    url = urljoin(base, "api/tags")
    r = requests.get(url, timeout=3)
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}")


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
        for name, fn in [
            ("postgres", _check_postgres),
            ("kafka", _check_kafka),
            ("qdrant", _check_qdrant),
            ("ollama", _check_ollama),
        ]:
            try:
                fn()
            except Exception as e:
                failures.append({"dep": name, "error": str(e)})

        if failures:
            resp.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return {"ready": False, "failures": failures}
        return {"ready": True}

    return app


def _run(port: int):
    import uvicorn

    uvicorn.run(make_app(), host="0.0.0.0", port=port, log_level="info")


def start_ops_server_in_thread(port: int):
    t = threading.Thread(target=_run, args=(port,), daemon=True)
    t.start()
    return t