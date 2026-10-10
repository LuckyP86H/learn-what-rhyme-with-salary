#!/usr/bin/env python3
"""Show what worker_prefetch_multiplier does to fairness. No failures involved.

Runs a call where two of the chunks are very slow, samples RabbitMQ's view of the heavy_processing
queue every second (ready vs unacked messages, per-consumer prefetch_count), and prints which worker
ran which chunk plus how long each worker sat idle.

With multiplier 4 each worker *reserves* 4 messages up front. A worker stuck on a slow chunk keeps
the ones queued behind it hostage while its neighbour finishes everything else and goes idle.
With multiplier 1 a worker only holds what it is running, so work flows to whoever is free.

Standard library only; run from the repo root (normally via `make test-prefetch`).
"""
from __future__ import annotations

import argparse
import base64
import json
import time
import urllib.parse
import urllib.request


def get(url: str, auth: tuple[str, str] | None = None) -> dict | list:
    req = urllib.request.Request(url)
    if auth:
        req.add_header("Authorization", "Basic " + base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode())
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def post(url: str, body: dict) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers={"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8100")
    ap.add_argument("--rabbitmq", default="http://localhost:15672")
    ap.add_argument("--chunks", type=int, default=8)
    ap.add_argument("--slow", default="0,2", help="comma-separated chunk indexes that take ~22s")
    args = ap.parse_args()
    auth = ("pipeline", "pipeline")
    vhost_q = f"{args.rabbitmq}/api/queues/%2F/heavy_processing"

    cfg = get(f"{args.api}/config")
    faults = {int(i): "slow" for i in args.slow.split(",") if i}
    call_id = post(f"{args.api}/calls", {"title": "prefetch demo", "num_chunks": args.chunks, "faults": faults})["call_id"]
    # RabbitMQ 4.x no longer reports Celery's (global-QoS) prefetch, so show the configured reservation and
    # let the *unacked* column below prove it: each worker reserves multiplier x concurrency messages.
    print(f"worker_prefetch_multiplier={cfg['worker_prefetch_multiplier']}  acks_late={cfg['task_acks_late']}; "
          f"{args.chunks} chunks, slow={sorted(faults)}; 2 workers x 1 process "
          f"=> each worker reserves {cfg['worker_prefetch_multiplier']} message(s)")
    print(f"{'t':>4}  {'ready':>5} {'unacked':>7}  {'running':>7} {'done':>4}")
    start = time.monotonic()
    state: dict = {}
    while time.monotonic() - start < 240:
        state = get(f"{args.api}/calls/{call_id}")  # type: ignore[assignment]
        q = get(vhost_q, auth)
        chunks = state["chunks"]
        running = sum(c["status"] == "PROCESSING" for c in chunks)
        done = sum(c["status"] == "COMPLETED" for c in chunks)
        print(f"{time.monotonic() - start:4.0f}  {q.get('messages_ready', 0):5d} {q.get('messages_unacknowledged', 0):7d}  "
              f"{running:7d} {done:4d}")
        if state["status"] in ("COMPLETED", "FAILED"):
            break
        time.sleep(2)

    makespan = time.monotonic() - start
    by_worker: dict[str, list[dict]] = {}
    for c in state["chunks"]:
        by_worker.setdefault(c["worker"] or "?", []).append(c)
    print(f"\ncall {state['status']} in {makespan:.0f}s. Who ran what (in start order), and how long each worker was idle:")
    first = min(c["started_at"] for c in state["chunks"] if c["started_at"])
    last = max(c["completed_at"] for c in state["chunks"] if c["completed_at"])
    span = (_ts(last) - _ts(first))
    for worker, cs in sorted(by_worker.items()):
        cs.sort(key=lambda c: c["started_at"])
        busy = sum(_ts(c["completed_at"]) - _ts(c["started_at"]) for c in cs if c["completed_at"])
        order = ", ".join(f"c{c['index']}{'*' if c['fault'] == 'slow' else ''}" for c in cs)
        print(f"  {worker:<22} ran [{order}]  busy {busy:4.0f}s  idle {max(span - busy, 0):4.0f}s")
    print("  (* = slow chunk). Total processing span:", f"{span:.0f}s")


def _ts(iso: str) -> float:
    from datetime import datetime

    return datetime.fromisoformat(iso).timestamp()


if __name__ == "__main__":
    main()
