"""HTTP API + Server-Sent Events for the UI. Read-only: nothing here can change a Celery deployment.

Streaming model is snapshot + delta, so there is nothing to replay when a browser reconnects:
  * on connect: one `snapshot` event (states table, workers, all run summaries)
  * afterwards: `run` events carrying the FULL view of any run that changed (small, idempotent)
  * every tick we also re-evaluate runs that are still active, because some states are derived from
    the *absence* of events (worker lost, ETA reached) and no event arrives to announce them.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

from celery_viz.derive import run_summary, run_view, workers_view
from celery_viz.states import load_states
from celery_viz.store import RunStore

KEEPALIVE_SECONDS = 15.0
_ACTIVE = ("RUNNING", "ATTENTION")


def _sse(event: str, data: Any, event_id: int | None = None) -> str:
    head = f"id: {event_id}\n" if event_id is not None else ""
    return f"{head}event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _snapshot(store: RunStore) -> dict[str, Any]:
    with store.lock:
        runs = sorted(store.runs.values(), key=lambda r: r.updated_local, reverse=True)
        return {
            "now": store.now(),
            "states": load_states().to_json(),
            "workers": workers_view(store),
            "runs": [run_summary(store, r) for r in runs],
        }


async def event_stream(store: RunStore, is_disconnected: Callable[[], Awaitable[bool]], tick: float = 1.0,
                       max_ticks: int | None = None) -> AsyncIterator[str]:
    """The SSE body. A plain async generator so it can be tested without a network."""
    loop = asyncio.get_running_loop()
    wake = asyncio.Event()
    pending: set[str | None] = set()

    def on_change(run_id: str | None) -> None:  # called from the collector thread
        pending.add(run_id)
        loop.call_soon_threadsafe(wake.set)

    store.listeners.append(on_change)
    try:
        snap = _snapshot(store)
        yield _sse("snapshot", snap, store.seq)
        sent: dict[str, str] = {}
        workers_sent = json.dumps(snap["workers"], default=str)
        last_write = time.monotonic()
        ticks = 0
        while not await is_disconnected():
            try:
                await asyncio.wait_for(wake.wait(), timeout=tick)
            except TimeoutError:
                pass
            wake.clear()
            changed = {r for r in pending if r}
            pending.clear()
            out: list[str] = []
            with store.lock:
                for run in list(store.runs.values()):
                    view = run_view(store, run)
                    # Re-send runs that changed, plus any still-active run (derived states move with the clock).
                    if run.id in changed or view["status"] in _ACTIVE or run.id not in sent:
                        payload = json.dumps(view, default=str)
                        if sent.get(run.id) != payload:
                            sent[run.id] = payload
                            out.append(_sse("run", view, store.seq))
                workers = workers_view(store)
            workers_payload = json.dumps(workers, default=str)
            if workers_payload != workers_sent:
                workers_sent = workers_payload
                out.append(_sse("workers", workers, store.seq))
            for chunk in out:
                yield chunk
            if out:
                last_write = time.monotonic()
            elif time.monotonic() - last_write > KEEPALIVE_SECONDS:
                yield ": keepalive\n\n"
                last_write = time.monotonic()
            ticks += 1
            if max_ticks is not None and ticks >= max_ticks:
                return
    finally:
        if on_change in store.listeners:
            store.listeners.remove(on_change)


def create_app(store: RunStore, source: Any | None = None, mode: str = "live") -> FastAPI:
    """`source` is anything with .status() (an EventCollector or ReplayDriver)."""
    app = FastAPI(title="celery-viz", description="Read-only Celery workflow visualizer API.")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "mode": mode, "runs": len(store.runs), "tasks": len(store.tasks), "workers": len(store.workers),
                "dropped_events": store.dropped, "source": source.status() if source else None}

    @app.get("/api/states")
    def states() -> dict[str, Any]:
        return load_states().to_json()

    @app.get("/api/runs")
    def runs(limit: int = 50) -> list[dict[str, Any]]:
        with store.lock:
            ordered = sorted(store.runs.values(), key=lambda r: r.updated_local, reverse=True)[: max(1, min(limit, 500))]
            return [run_summary(store, r) for r in ordered]

    @app.get("/api/runs/{run_id}")
    def run(run_id: str) -> dict[str, Any]:
        with store.lock:
            found = store.runs.get(run_id)
            if found is None:
                raise HTTPException(404, "unknown run (it may have been evicted from the bounded store)")
            return run_view(store, found)

    @app.get("/api/workers")
    def workers() -> list[dict[str, Any]]:
        with store.lock:
            return workers_view(store)

    @app.get("/api/stream")
    async def stream(request: Request) -> StreamingResponse:
        return StreamingResponse(
            event_stream(store, request.is_disconnected),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},  # no proxy buffering
        )

    return app
