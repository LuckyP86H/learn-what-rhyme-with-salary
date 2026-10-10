"""The "Task Police": a periodic sweep that finds work that silently stopped and re-dispatches it.

This is the band-aid many teams grow around Celery. Read it as a *symptom*: it exists because a
broker message is not a durable record of "this work must finish". If a message is lost (early ack +
worker death, an expired queue, a purged broker) Celery never finds out, so something outside Celery
has to notice that a call has made no progress and push it again.

Compare with Temporal (see docs/comparison.md): there the workflow history IS the durable record, and an
activity that stops heartbeating is rescheduled by the server itself. No police required.

Caveats you can see in this file:
  * It only catches what it was written to look for (here: no heartbeat/progress for N seconds).
  * Re-dispatching a *slow* (not dead) call creates duplicate work. Idempotency makes that safe, not free.
  * It runs on the same broker it is supposed to rescue. A dead broker means a dead police force.
  * Thresholds are guesses: too short = duplicate work, too long = slow recovery.
"""
from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import datetime

from celery import Task

from app import repository as repo
from app.celery_app import celery_app
from app.repository import Actor
from app.tasks.pipeline import build_workflow
from config import settings
from database import utcnow

log = logging.getLogger(__name__)


def is_stalled(updated_at: datetime, chunk_activity: Iterable[datetime], now: datetime, threshold_seconds: float) -> bool:
    """True if nothing about this call (the row itself or any chunk heartbeat) moved for `threshold_seconds`."""
    last_sign_of_life = max([updated_at, *chunk_activity])
    return (now - last_sign_of_life).total_seconds() >= threshold_seconds


@celery_app.task(bind=True, name="tasks.police_sweep")
def police_sweep(self: Task) -> dict[str, list[str]]:
    actor = Actor(self.name, self.request.id, self.request.hostname)
    now = utcnow()
    recovered: list[str] = []
    abandoned: list[str] = []

    for snap in repo.snapshot_active_calls():
        if not is_stalled(snap.updated_at, snap.chunk_activity, now, settings.stall_threshold_seconds):
            continue
        if snap.recovery_count >= settings.max_recoveries:
            # Don't recover forever: if three re-dispatches didn't help, a human needs to look.
            repo.fail_call(snap.call_id, f"stalled; gave up after {snap.recovery_count} police recoveries", actor)
            abandoned.append(snap.call_id)
            continue

        reason = f"no progress for >= {settings.stall_threshold_seconds:.0f}s while {snap.status}"
        plan = repo.begin_recovery(snap.call_id, actor, reason)
        if plan is None:
            continue
        log.warning("recovering stalled call", extra={"call_id": snap.call_id, "reason": reason, "recovery": plan.attempt_number})
        if plan.needs_ingest:
            celery_app.send_task("tasks.ingest_call", args=[snap.call_id])
        else:
            # A brand-new chord over the unfinished chunks. The original chord can never complete
            # (its lost header task will never report in), which is exactly why we start a new one.
            build_workflow(snap.call_id, plan.incomplete_chunk_ids).apply_async()
        recovered.append(snap.call_id)

    return {"recovered": recovered, "abandoned": abandoned}
