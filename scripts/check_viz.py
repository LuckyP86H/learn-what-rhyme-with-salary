#!/usr/bin/env python3
"""Validate the live visualizer against ground truth (the pipeline's own database).

Submits N calls, waits for them to finish, then checks that the monitor (celery_viz) shows the same
outcome for every run and every chunk. Run it after any change to the collector or reducer:

    python3 scripts/check_viz.py            # 15 calls x 3 chunks, about 3 minutes
    python3 scripts/check_viz.py --calls 4 --chunks 2

Exit code 0 only if the monitor agrees with the database on every run. Standard library only.
"""
from __future__ import annotations

import argparse
import collections
import json
import sys
import time
import urllib.request


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.load(resp)


def post(url: str, body: dict):
    req = urllib.request.Request(url, json.dumps(body).encode(), {"content-type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--api", default="http://localhost:8100")
    ap.add_argument("--viz", default="http://localhost:8200")
    ap.add_argument("--calls", type=int, default=15)
    ap.add_argument("--chunks", type=int, default=3)
    ap.add_argument("--timeout", type=float, default=600)
    args = ap.parse_args()

    try:
        health = get(f"{args.viz}/api/health")
    except OSError as exc:
        print(f"cannot reach the visualizer at {args.viz} ({exc}). Is the stack up? Try: make up", file=sys.stderr)
        return 2
    if not health["source"] or not health["source"].get("connected"):
        print(f"the visualizer is not connected to a broker: {health['source']}", file=sys.stderr)
        return 2

    known = {r["id"] for r in get(f"{args.viz}/api/runs?limit=500")}
    ids = [post(f"{args.api}/calls", {"title": f"check {i}", "num_chunks": args.chunks})["call_id"] for i in range(args.calls)]
    total = args.calls * args.chunks
    print(f"submitted {args.calls} calls ({total} chunks); waiting for the database to say they are done...")

    start = time.time()
    status = collections.Counter()
    while time.time() - start < args.timeout:
        status = collections.Counter(get(f"{args.api}/calls/{c}")["status"] for c in ids)
        if status["COMPLETED"] + status["FAILED"] == args.calls:
            break
        time.sleep(4)
    print(f"database ({time.time() - start:.0f}s): {dict(status)}")
    time.sleep(8)  # let the last events arrive

    runs = []
    for summary in get(f"{args.viz}/api/runs?limit=500"):
        if summary["id"] in known:
            continue
        run = get(f"{args.viz}/api/runs/{summary['id']}")
        if any(t["name"] == "tasks.ingest_call" for t in run["tasks"]):
            runs.append(run)
    chunks = [t for r in runs for t in r["tasks"] if t["name"] == "tasks.analyze_chunk"]
    chunk_states = collections.Counter(t["state"] for t in chunks)
    run_states = collections.Counter(r["status"] for r in runs)
    print(f"monitor : {len(runs)} runs {dict(run_states)}; chunks {dict(chunk_states)}")

    expected_ok = status["COMPLETED"]
    problems = []
    if len(runs) != args.calls:
        problems.append(f"monitor shows {len(runs)} runs, expected {args.calls}")
    if run_states["COMPLETED"] != expected_ok:
        problems.append(f"monitor shows {run_states['COMPLETED']} COMPLETED runs, database says {expected_ok}")
    if status["FAILED"] == 0 and chunk_states["SUCCESS"] != total:
        problems.append(f"monitor shows {chunk_states['SUCCESS']}/{total} chunks as SUCCESS (lost terminal events?)")
    stuck = [t for t in chunks if t["state"] in ("STARTED", "SUSPECTED_HUNG", "ENDED_SILENTLY", "RECEIVED", "QUEUED")]
    if stuck and status["FAILED"] == 0:
        problems.append(f"{len(stuck)} chunks are not terminal in the monitor")
    if problems:
        print("FAIL:\n  - " + "\n  - ".join(problems))
        return 1
    print("PASS: the monitor agrees with the database on every run and chunk.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
