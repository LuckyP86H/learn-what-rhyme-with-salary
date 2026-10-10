"""The Celery Canvas pipeline:  ingest_call ──▶ chord( group(analyze_chunk × N) ──▶ merge_and_finalize )

Why `ingest_call` ends with `self.replace(...)` instead of the textbook
`chain(ingest_call.s() | chord(group(...), ...))`:
the chunk ids do not exist until ingest creates them, so the group cannot be built up front.
`replace` lets a task swap itself for a workflow that is computed at runtime.

There is no `.get()` / `allow_join_result` anywhere: no worker ever sits blocked waiting for another
task. Completion is driven entirely by the chord callback.
"""
from __future__ import annotations

import logging
import os
import random
import signal
import time
from typing import Any

from celery import Task, chord, group
from celery.canvas import Signature
from celery.exceptions import Ignore, Reject, SoftTimeLimitExceeded

from app import repository as repo
from app.analysis import simulate_chunk
from app.celery_app import celery_app
from app.errors import LeaseLost, PermanentError, TransientError
from app.repository import Actor, Claim, ClaimOutcome
from config import settings

log = logging.getLogger(__name__)

# Faults you can inject per chunk through POST /calls {"faults": {"2": "crash"}}. Each one exists to
# exercise exactly one Celery reliability feature:
#   transient  fails on attempts 1-2, then works  -> retry with exponential backoff
#   permanent  always raises PermanentError       -> Reject(requeue=False) -> dead-letter queue
#   crash      SIGKILLs its own pool child        -> reject_on_worker_lost + the attempts cap (poison pill)
#   hang       sleeps forever on attempt 1        -> soft time limit -> retry
#   bug        raises an unexpected exception     -> task FAILURE -> chord error callback
#   slow       takes SLOW_CHUNK_SECONDS           -> prefetch / fair-dispatch experiment
FAULTS = ("transient", "permanent", "crash", "hang", "bug", "slow")


def _actor(task: Task) -> Actor:
    return Actor(task.name, task.request.id, task.request.hostname)


def backoff_seconds(attempt: int) -> float:
    """Exponential backoff with jitter: ~2s, 4s, 8s ... capped. Jitter stops retries marching in lockstep."""
    delay = min(settings.retry_backoff_base**attempt, settings.retry_backoff_max)
    return delay * random.uniform(0.75, 1.25)


# --------------------------------------------------------------------------------------------------
# Step 1: ingest
# --------------------------------------------------------------------------------------------------
@celery_app.task(bind=True, name="tasks.ingest_call", max_retries=None)
def ingest_call(self: Task, call_id: str) -> None:
    """Validate metadata, create the chunk rows, then replace ourselves with the analysis chord."""
    actor = _actor(self)
    try:
        chunk_ids = repo.ingest(call_id, actor)
    except repo.InvalidCall as exc:
        # Permanent: retrying can't fix bad metadata. Record it, then reject WITHOUT requeue so the
        # broker parks the message in the dead-letter queue instead of looping.
        log.warning("invalid call metadata", extra={"call_id": call_id, "error": str(exc)})
        repo.fail_call(call_id, f"invalid metadata: {exc}", actor)
        raise Reject(str(exc), requeue=False)

    log.info("call ingested", extra={"call_id": call_id, "chunks": len(chunk_ids)})
    raise self.replace(build_workflow(call_id, chunk_ids))


def build_workflow(call_id: str, chunk_ids: list[str]) -> Signature:
    """The fan-out/fan-in graph. Also used by the Task Police to re-dispatch only what is unfinished."""
    callback = merge_and_finalize.s(call_id).on_error(on_pipeline_failure.s(call_id))
    if not chunk_ids:
        # Everything already finished but the callback never ran (e.g. it was lost): just run it.
        return merge_and_finalize.s([], call_id).on_error(on_pipeline_failure.s(call_id))
    return chord(group([analyze_chunk.s(cid) for cid in chunk_ids]), callback)


# --------------------------------------------------------------------------------------------------
# Step 2: analyze one chunk (the long-running, Spot-evictable part)
# --------------------------------------------------------------------------------------------------
@celery_app.task(bind=True, name="tasks.analyze_chunk", max_retries=None)
def analyze_chunk(self: Task, chunk_id: str) -> dict[str, Any]:
    """Idempotent: safe to run any number of times, sequentially or concurrently, for the same chunk.

    max_retries=None because *we* own the retry budget (`attempts` in PostgreSQL), which also counts
    crashes and redeliveries that Celery's own retry counter cannot see.
    """
    actor = _actor(self)
    redelivered = bool((self.request.delivery_info or {}).get("redelivered"))
    claim = repo.claim_chunk(chunk_id, actor, redelivered=redelivered)

    if claim.outcome is ClaimOutcome.ALREADY_DONE:
        # Idempotency in action: a redelivery (or a police re-dispatch) for finished work.
        log.info("chunk already completed; skipping", extra=_ctx(claim))
        return claim.result or {}
    if claim.outcome is ClaimOutcome.BUSY:
        # Someone else holds a live lease. Do NOT return a result (the chord would count this as done).
        raise self.retry(countdown=settings.lease_retry_seconds)
    if claim.outcome in (ClaimOutcome.FAILED, ClaimOutcome.EXHAUSTED):
        raise Reject(f"chunk {claim.index} is permanently failed ({claim.outcome})", requeue=False)

    log.info("chunk started", extra=_ctx(claim, takeover=claim.takeover, redelivered=redelivered))
    try:
        result = _process_chunk(claim)
        if not repo.complete_chunk(claim, result, actor):
            raise LeaseLost()
    except LeaseLost:
        # Someone took the chunk over while we were slow/partitioned. Stand down quietly; the new
        # owner's completion is what the chord will count.
        log.warning("lease lost; abandoning chunk", extra=_ctx(claim))
        raise Ignore()
    except SoftTimeLimitExceeded:
        # The soft limit gave us a chance to clean up. Treat it as transient.
        error = f"soft time limit ({celery_app.conf.task_soft_time_limit}s) exceeded"
        repo.release_chunk(claim, error, actor)
        raise self.retry(exc=TransientError(error), countdown=backoff_seconds(claim.attempt))
    except TransientError as exc:
        repo.release_chunk(claim, str(exc), actor)
        log.warning("transient failure; retrying with backoff", extra=_ctx(claim, error=str(exc)))
        raise self.retry(exc=exc, countdown=backoff_seconds(claim.attempt))
    except PermanentError as exc:
        repo.fail_chunk(claim, str(exc), actor)
        log.error("permanent failure; rejecting to the DLQ", extra=_ctx(claim, error=str(exc)))
        # Reject(requeue=False) -> basic.nack -> RabbitMQ dead-letters the message. This only works
        # with acks_late=True: with early acks the message is already gone, so there is nothing to reject.
        raise Reject(str(exc), requeue=False)
    except Exception as exc:
        # A genuine bug. Record it, then let the task FAIL: Celery stores the exception and fires the
        # chord's error callback (on_pipeline_failure) instead of the success callback.
        repo.fail_chunk(claim, f"{type(exc).__name__}: {exc}", actor)
        log.exception("unexpected error in chunk", extra=_ctx(claim))
        raise
    return result


def _ctx(claim: Claim, **extra: Any) -> dict[str, Any]:
    return {"call_id": claim.call_id, "chunk_id": claim.chunk_id, "chunk_index": claim.index, "attempt": claim.attempt, **extra}


def _process_chunk(claim: Claim) -> dict[str, Any]:
    """Fake transcription + diarization: sleep, heartbeating each tick, and maybe misbehave halfway."""
    sim = simulate_chunk(claim.call_id, claim.index, settings.chunk_min_seconds, settings.chunk_max_seconds)
    duration = settings.slow_chunk_seconds if claim.fault == "slow" else sim.duration_s
    ticks = max(1, round(duration / settings.heartbeat_interval))
    assert claim.token
    for tick in range(ticks):
        time.sleep(settings.heartbeat_interval)
        # Renewing the lease is what tells the world "I'm alive". If this returns False someone fenced us out.
        if not repo.heartbeat(claim.chunk_id, claim.token, (tick + 1) / ticks):
            raise LeaseLost()
        if tick == ticks // 2:
            _inject_fault(claim)
    return sim.result


def _inject_fault(claim: Claim) -> None:
    fault, attempt = claim.fault, claim.attempt
    if fault == "transient" and attempt <= 2:
        raise TransientError(f"upstream ASR timeout (attempt {attempt})")
    if fault == "permanent":
        raise PermanentError("corrupt audio: unsupported codec")
    if fault == "crash":
        # We are in the prefork pool child. SIGKILL here is what the kernel OOM-killer does: the child
        # vanishes without cleanup, the parent worker survives and notices (WorkerLostError).
        os.kill(os.getpid(), signal.SIGKILL)
    if fault == "hang" and attempt == 1:
        time.sleep(3600)  # interrupted by the soft time limit
    if fault == "bug":
        raise RuntimeError("'NoneType' object has no attribute 'split'")


# --------------------------------------------------------------------------------------------------
# Step 3: chord callback and error callback
# --------------------------------------------------------------------------------------------------
@celery_app.task(bind=True, name="tasks.merge_and_finalize", max_retries=None)
def merge_and_finalize(self: Task, results: list[dict[str, Any]], call_id: str) -> dict[str, Any]:
    """Runs once every chunk in the header succeeded. Celery prepends the header results as `results`."""
    outcome = repo.finalize_call(call_id, _actor(self), received=len(results))
    log.info("merge finished", extra={"call_id": call_id, "outcome": str(outcome), "received": len(results)})
    return {"call_id": call_id, "outcome": str(outcome)}


@celery_app.task(name="tasks.on_pipeline_failure")
def on_pipeline_failure(request: Any, exc: BaseException, traceback: Any, call_id: str) -> None:
    """Chord error callback: fires when a header task FAILED (raised) rather than succeeded.

    Because this signature takes more than one argument, Celery invokes it *in-process* as
    `on_pipeline_failure(request, exc, traceback, *bound_args)`: `request` describes the failed task and
    our bound `call_id` arrives last. (An errback taking only `task_id` would instead be sent as a task.)

    Note what does NOT trigger this: `Reject(requeue=False)`. A rejected task never stores a result, so
    the chord is told nothing. That is why permanent failures also mark the call FAILED in PostgreSQL
    themselves; the database, not the chord, is the source of truth.
    """
    log.error("pipeline failed", extra={"call_id": call_id, "failed_task": request.id, "error": repr(exc)})
    repo.fail_call(call_id, f"pipeline failed: {exc!r}", Actor("tasks.on_pipeline_failure", request.id))
