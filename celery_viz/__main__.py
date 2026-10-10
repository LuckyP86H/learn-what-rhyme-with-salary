"""CLI:  python -m celery_viz --broker amqp://...        (live)
        python -m celery_viz --replay recording.jsonl   (no broker needed)
"""
from __future__ import annotations

import argparse
import logging
import os

import uvicorn

from celery_viz.collector import EventCollector, ReplayDriver
from celery_viz.config import VizConfig
from celery_viz.redaction import RedactionPolicy
from celery_viz.replay import load_events
from celery_viz.server import create_app
from celery_viz.store import RunStore


def build(args: argparse.Namespace):
    config = VizConfig(hung_after_seconds=args.hung_after, max_runs=args.max_runs,
                       min_lost_after_seconds=args.lost_after, drain_grace_seconds=args.drain_grace)
    policy = RedactionPolicy(keep_args=args.keep_args, keep_result=args.keep_result)
    if args.replay:
        store = RunStore(config, live=False)
        source = ReplayDriver(list(load_events(args.replay)), store, policy, speed=args.speed)
        return create_app(store, source, mode="replay"), source
    store = RunStore(config, live=True)
    if not args.broker:
        raise SystemExit("--broker (or the BROKER_URL environment variable) is required unless --replay is given")
    source = EventCollector(args.broker, store, policy)
    return create_app(store, source, mode="live"), source


def main() -> None:
    ap = argparse.ArgumentParser(prog="celery_viz", description="Read-only Celery workflow visualizer.")
    ap.add_argument("--broker", default=os.environ.get("BROKER_URL"), help="broker URL of the Celery deployment to observe")
    ap.add_argument("--replay", help="replay a recorded events file (one JSON object per line) instead of connecting")
    ap.add_argument("--speed", type=float, default=1.0, help="replay speed multiplier")
    ap.add_argument("--host", default="127.0.0.1", help="bind address; the API has no auth, keep it on loopback unless proxied")
    ap.add_argument("--port", type=int, default=8200)
    ap.add_argument("--hung-after", type=float, default=None, help="flag tasks running longer than this many seconds as suspected hung")
    ap.add_argument("--lost-after", type=float, default=12.0, help="never call a worker lost before this many seconds of silence (it is also at least 3x the worker's own heartbeat period)")
    ap.add_argument("--drain-grace", type=float, default=35.0, help="how long a worker that announced shutdown may keep running tasks before they count as lost (match your orchestrator's stop grace period)")
    ap.add_argument("--max-runs", type=int, default=500)
    ap.add_argument("--keep-args", action="store_true", help="keep task args/kwargs (default: dropped; may contain personal data)")
    ap.add_argument("--keep-result", action="store_true", help="keep task results (default: dropped)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    app, source = build(args)
    source.start()
    try:
        uvicorn.run(app, host=args.host, port=args.port, log_level="warning")
    finally:
        source.stop()


if __name__ == "__main__":
    main()
