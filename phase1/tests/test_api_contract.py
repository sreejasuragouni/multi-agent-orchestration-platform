import os
import time

import requests

API_BASE = os.environ.get("API_BASE", "http://localhost:8000")

def test_task_flow_contract():
    # health
    r = requests.get(f"{API_BASE}/health", timeout=5)
    r.raise_for_status()

    # submit
    r = requests.post(f"{API_BASE}/tasks", json={"task": "contract test task"}, timeout=10)
    r.raise_for_status()
    task_id = r.json()["task_id"]

    # poll for final completion
    deadline = time.time() + 180
    final_done = False
    while time.time() < deadline:
        s = requests.get(f"{API_BASE}/tasks/{task_id}", timeout=10)
        if s.status_code == 200:
            data = s.json()
            final = [x for x in data.get("subtasks", []) if x.get("subtask_id") == "final"]
            if final and final[0].get("status") == "COMPLETED":
                final_done = True
                break
        time.sleep(2)

    assert final_done, "final stage did not complete in time"

    # final should exist
    f = requests.get(f"{API_BASE}/tasks/{task_id}/final", timeout=10)
    f.raise_for_status()
    final_text = f.json().get("final", "")
    assert isinstance(final_text, str) and len(final_text) > 20