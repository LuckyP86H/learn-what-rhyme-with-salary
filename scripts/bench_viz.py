#!/usr/bin/env python3
"""Measure what the visualizer costs: event ingestion, a pass over many runs, and one huge run.

    python scripts/bench_viz.py                 # 500 runs x 10 tasks, plus a 1500-task run
    python scripts/bench_viz.py --runs 100 --fan-out 500

Needs no broker: it builds a store from synthetic events. Used by `make bench-viz`.
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from celery_viz.derive import run_view  # noqa: E402
from celery_viz.store import RunStore  # noqa: E402


class Feeder:
    def __init__(self, store: RunStore):
        self.store, self.clock = store, 1000.0

    def event(self, etype: str, uuid: str, **kw) -> None:
        self.clock += 0.001
        self.store.apply({"type": etype, "uuid": uuid, "hostname": "w@1", "retries": 0, "timestamp": self.clock, "local_received": self.clock, **kw})

    def finished_task(self, uuid: str, name: str, root: str, parent: str | None) -> None:
        extra = {"name": name, "root_id": root, "parent_id": parent}
        self.event("task-sent", uuid, **extra)
        self.event("task-received", uuid, **extra)
        self.event("task-started", uuid)
        self.event("task-succeeded", uuid, runtime=1.0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--runs", type=int, default=500, help="number of runs in the many-runs test")
    ap.add_argument("--tasks-per-run", type=int, default=10)
    ap.add_argument("--fan-out", type=int, default=1500, help="parallel tasks in the one-huge-run test (the store caps a run at 2000 tasks)")
    args = ap.parse_args()

    store = RunStore(live=False)
    feed = Feeder(store)
    start = time.perf_counter()
    for r in range(args.runs):
        root = f"root{r}"
        for k in range(args.tasks_per_run):
            feed.finished_task(root if k == 0 else f"{root}-{k}", f"tasks.t{k}", root, None if k == 0 else root)
    ingest = time.perf_counter() - start
    events = args.runs * args.tasks_per_run * 4
    print(f"ingest: {events:,} events in {ingest:.2f}s ({events / ingest:,.0f} events/s)")

    start = time.perf_counter()
    for run in list(store.runs.values()):
        run_view(store, run)
    print(f"views:  one pass over {args.runs} runs x {args.tasks_per_run} tasks = {(time.perf_counter() - start) * 1000:.0f} ms (the server does this once a second)")

    big = RunStore(live=False)
    feed = Feeder(big)
    feed.event("task-sent", "big", name="tasks.parent", root_id="big")
    for i in range(args.fan_out):
        feed.finished_task(f"c{i}", "tasks.child", "big", "big")
    feed.event("task-sent", "big", name="tasks.join", root_id="big", parent_id=f"c{args.fan_out - 1}")
    start = time.perf_counter()
    view = run_view(big, big.runs["big"])
    print(f"huge:   one run with {len(view['tasks'])} tasks = {(time.perf_counter() - start) * 1000:.0f} ms to render ({len(view['edges'])} edges)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
