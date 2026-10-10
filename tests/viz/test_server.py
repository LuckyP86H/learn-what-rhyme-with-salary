import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from celery_viz.collector import ReplayDriver
from celery_viz.server import create_app, event_stream
from celery_viz.store import RunStore

from .conftest import build_store, events_of


@pytest.fixture(scope="module")
def client():
    store = build_store("celery5_pipeline")
    return TestClient(create_app(store, mode="replay"))


def test_health_reports_mode_and_sizes(client):
    body = client.get("/api/health").json()
    assert body["status"] == "ok" and body["mode"] == "replay"
    assert body["runs"] > 0 and body["tasks"] > 0 and body["workers"] > 0


def test_states_endpoint_serves_the_table_the_ui_renders(client):
    body = client.get("/api/states").json()
    keys = {s["key"] for s in body["states"]}
    assert {"QUEUED", "STARTED", "SUCCESS", "WORKER_LOST", "ENDED_SILENTLY", "UNKNOWN"} <= keys
    assert [s["key"] for s in body["stations"]][:3] == ["queue", "reserved", "running"]


def test_runs_are_listed_newest_first_and_can_be_fetched(client):
    runs = client.get("/api/runs").json()
    assert runs == sorted(runs, key=lambda r: r["updated_at"], reverse=True)
    detail = client.get(f"/api/runs/{runs[0]['id']}").json()
    assert detail["id"] == runs[0]["id"] and detail["tasks"] and "edges" in detail


def test_unknown_run_is_a_404_not_a_crash(client):
    assert client.get("/api/runs/nope").status_code == 404


def test_run_list_limit_is_clamped(client):
    assert len(client.get("/api/runs?limit=1").json()) == 1
    assert len(client.get("/api/runs?limit=99999").json()) <= 500


def test_no_payload_data_leaks_through_the_api(client):
    text = json.dumps(client.get("/api/runs").json()) + json.dumps([client.get(f"/api/runs/{r['id']}").json() for r in client.get("/api/runs").json()])
    for forbidden in ("kwargs", "traceback", '"args"', '"result"'):
        assert forbidden not in text


# ---- SSE ----------------------------------------------------------------------------------------------
def collect(store, ticks=2, feed=None):
    async def go():
        async def never():
            return False

        out = []
        async for chunk in event_stream(store, never, tick=0.05, max_ticks=ticks):
            out.append(chunk)
            if feed and len(out) == 1:
                feed()
        return out

    return asyncio.run(go())


def parse(chunks):
    events = []
    for chunk in chunks:
        if chunk.startswith(":"):
            continue
        lines = dict(line.split(": ", 1) for line in chunk.strip().split("\n") if ": " in line)
        events.append((lines["event"], json.loads(lines["data"])))
    return events


def test_stream_starts_with_a_full_snapshot():
    store = build_store("celery5_pipeline")
    events = parse(collect(store, ticks=1))
    kind, snap = events[0]
    assert kind == "snapshot"
    assert snap["states"]["states"] and snap["runs"] and snap["workers"]


def test_stream_pushes_a_run_when_events_arrive():
    store = RunStore()
    sent = {"type": "task-sent", "uuid": "r1", "name": "t.x", "root_id": "r1", "retries": 0, "timestamp": 1.0, "local_received": 1.0}

    def feed():
        store.apply(sent)

    events = parse(collect(store, ticks=3, feed=feed))
    kinds = [k for k, _ in events]
    assert kinds[0] == "snapshot" and "run" in kinds
    run = next(d for k, d in events if k == "run")
    assert run["id"] == "r1" and run["tasks"][0]["state"] == "QUEUED"


def test_stream_announces_a_worker_lost_with_no_new_task_event():
    # The whole point of the tick: WORKER_LOST is the ABSENCE of events, so something must re-evaluate.
    store = RunStore(live=True)
    base = __import__("time").time()
    for etype, extra in (("task-sent", {"name": "t.x", "root_id": "r", "retries": 0}), ("task-received", {"name": "t.x", "root_id": "r", "retries": 0}), ("task-started", {})):
        store.apply({"type": etype, "uuid": "r", "hostname": "w@1", "timestamp": base - 100, "local_received": base - 100, **extra})
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 2, "timestamp": base - 100, "local_received": base - 100})
    events = parse(collect(store, ticks=2))
    run = next(d for k, d in events if k == "run")
    assert run["tasks"][0]["state"] == "WORKER_LOST", "100 s of silence from a 2 s heartbeat must be flagged"
    assert run["status"] == "ATTENTION"


def test_stream_unregisters_its_listener_when_the_client_goes_away():
    store = RunStore()
    collect(store, ticks=1)
    assert store.listeners == []


# ---- replay driver ---------------------------------------------------------------------------------------
def test_replay_driver_feeds_the_store_and_finishes():
    store = RunStore()
    driver = ReplayDriver(events_of("celery5_pipeline")[:60], store, speed=1000.0)
    driver.start()
    driver.join(timeout=10)
    assert driver.finished and driver.events_seen == 60
    assert store.tasks
    assert driver.status()["replay_finished"] is True
