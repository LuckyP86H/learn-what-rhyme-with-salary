"""Every PostgreSQL state transition in one place.

Design rules, because Celery gives us *at-least-once* delivery and nothing stronger:

1. A task may run twice, concurrently, or after its worker was presumed dead. So a task never trusts
   its own memory: it first *claims* its work row, and every later write is fenced by that claim.
2. Terminal states (COMPLETED / FAILED) are absorbing. Re-running a finished step is a cheap no-op.
3. Each function is one short transaction; nothing holds a DB lock while "processing" for 10 seconds.
"""
from __future__ import annotations

import enum
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, NamedTuple

from sqlalchemy import select, update

from app.analysis import merge_results
from config import settings
from database import (
    TERMINAL_CALL_STATUSES,
    Call,
    CallChunk,
    CallStatus,
    ChunkStatus,
    TaskExecutionLog,
    session_scope,
    utcnow,
)

log = logging.getLogger(__name__)

MAX_CHUNKS = 50


class Actor(NamedTuple):
    """Who is doing this, for the audit trail."""

    task_name: str
    task_id: str | None = None
    worker: str | None = None


class InvalidCall(Exception):
    """Call metadata can never be valid: a permanent error."""


def _aware(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _log(s: Any, call_id: str, actor: Actor, event: str, detail: str | None = None, chunk_id: str | None = None) -> None:
    s.add(
        TaskExecutionLog(
            call_id=call_id,
            chunk_id=chunk_id,
            task_name=actor.task_name,
            celery_task_id=actor.task_id,
            worker=actor.worker,
            event=event,
            detail=detail,
        )
    )


def log_event(call_id: str, actor: Actor, event: str, detail: str | None = None, chunk_id: str | None = None) -> None:
    with session_scope() as s:
        _log(s, call_id, actor, event, detail, chunk_id)


def _fail_call(s: Any, call_id: str, error: str) -> None:
    s.execute(
        update(Call)
        .where(Call.id == call_id, Call.status.not_in([str(x) for x in TERMINAL_CALL_STATUSES]))
        .values(status=CallStatus.FAILED, error=error)
    )


# --------------------------------------------------------------------------------------------------
# ingest
# --------------------------------------------------------------------------------------------------
def ingest(call_id: str, actor: Actor) -> list[str]:
    """Validate the call, create its chunk rows (idempotently) and return the ids still to be processed."""
    with session_scope() as s:
        call = s.get(Call, call_id, with_for_update=True)
        if call is None:
            raise InvalidCall(f"call {call_id} does not exist")
        if not call.title.strip():
            raise InvalidCall("title must not be empty")
        if not 1 <= call.num_chunks <= MAX_CHUNKS:
            raise InvalidCall(f"num_chunks must be between 1 and {MAX_CHUNKS}, got {call.num_chunks}")

        existing = {c.index: c for c in call.chunks}  # redelivery / police recovery: some rows may exist
        for i in range(call.num_chunks):
            if i not in existing:
                existing[i] = CallChunk(
                    call_id=call.id,
                    index=i,
                    audio_uri=f"s3://mock-audio/{call.id}/chunk-{i:03d}.wav",  # pretend we split the audio
                    fault=(call.faults or {}).get(str(i)),
                )
                s.add(existing[i])
        if call.status == CallStatus.PENDING:
            call.status = CallStatus.INGESTED
            _log(s, call.id, actor, "CALL_INGESTED", f"{call.num_chunks} chunks")
        s.flush()
        return [c.id for _, c in sorted(existing.items()) if c.status != ChunkStatus.COMPLETED]


def fail_call(call_id: str, error: str, actor: Actor) -> None:
    with session_scope() as s:
        _fail_call(s, call_id, error)
        _log(s, call_id, actor, "CALL_FAILED", error)


# --------------------------------------------------------------------------------------------------
# chunk lifecycle: claim -> heartbeat* -> complete | release | fail
# --------------------------------------------------------------------------------------------------
class ClaimOutcome(enum.StrEnum):
    CLAIMED = "CLAIMED"  # you own the chunk now; go process it
    ALREADY_DONE = "ALREADY_DONE"  # idempotency: someone finished it; just return the stored result
    BUSY = "BUSY"  # someone else holds a live lease: back off and look again later
    FAILED = "FAILED"  # chunk is permanently failed
    EXHAUSTED = "EXHAUSTED"  # too many attempts (poison pill): we just marked it FAILED


@dataclass(frozen=True)
class Claim:
    outcome: ClaimOutcome
    chunk_id: str
    call_id: str
    index: int
    fault: str | None
    attempt: int = 0
    token: str | None = None
    takeover: bool = False
    result: dict[str, Any] | None = field(default=None, repr=False)


def claim_chunk(chunk_id: str, actor: Actor, redelivered: bool = False) -> Claim:
    """Atomically decide whether this delivery may process the chunk.

    `redelivered` is RabbitMQ's own flag on the message ("I have delivered this before"). It is only
    recorded for the audit trail; correctness comes from the lease, not from trusting the flag.
    """
    with session_scope() as s:
        chunk = s.get(CallChunk, chunk_id, with_for_update=True)  # row lock: serialises racing deliveries
        if chunk is None:
            raise InvalidCall(f"chunk {chunk_id} does not exist")
        ident = {"chunk_id": chunk.id, "call_id": chunk.call_id, "index": chunk.index, "fault": chunk.fault}
        now = utcnow()

        if chunk.status == ChunkStatus.COMPLETED:
            _log(s, chunk.call_id, actor, "CHUNK_SKIPPED_ALREADY_DONE", f"redelivered={redelivered}", chunk.id)
            return Claim(ClaimOutcome.ALREADY_DONE, attempt=chunk.attempts, result=chunk.result, **ident)
        if chunk.status == ChunkStatus.FAILED:
            return Claim(ClaimOutcome.FAILED, attempt=chunk.attempts, **ident)

        takeover = False
        if chunk.status == ChunkStatus.PROCESSING:
            heartbeat_age = (now - _aware(chunk.heartbeat_at)).total_seconds() if chunk.heartbeat_at else 1e9
            if heartbeat_age < settings.lease_seconds:
                # A live owner exists (or died <lease_seconds ago). Don't double-process; don't report
                # success either, or the chord would fire early. Caller retries shortly.
                if redelivered:
                    # Only the broker's own redelivery is worth an audit row (retries would be noise): it
                    # is the proof that RabbitMQ requeued a dead worker's unacked message.
                    _log(s, chunk.call_id, actor, "CHUNK_REDELIVERED_BUSY",
                         f"redelivered=True; owner {chunk.worker} heartbeat {heartbeat_age:.1f}s ago "
                         f"(< {settings.lease_seconds:.0f}s lease), waiting for it to expire", chunk.id)
                return Claim(ClaimOutcome.BUSY, attempt=chunk.attempts, **ident)
            takeover = True  # owner stopped heart-beating: presumed dead

        if chunk.attempts >= settings.max_attempts:
            chunk.status = ChunkStatus.FAILED
            chunk.last_error = f"gave up after {chunk.attempts} attempts"
            _fail_call(s, chunk.call_id, f"chunk {chunk.index}: {chunk.last_error}")
            _log(s, chunk.call_id, actor, "CHUNK_EXHAUSTED", chunk.last_error, chunk.id)
            return Claim(ClaimOutcome.EXHAUSTED, attempt=chunk.attempts, **ident)

        previous_worker = chunk.worker
        chunk.attempts += 1
        chunk.status = ChunkStatus.PROCESSING
        chunk.claim_token = uuid.uuid4().hex
        chunk.worker = actor.worker
        chunk.heartbeat_at = now
        chunk.started_at = now
        chunk.progress = 0.0
        s.execute(
            update(Call).where(Call.id == chunk.call_id, Call.status == CallStatus.INGESTED).values(status=CallStatus.PROCESSING)
        )
        if takeover:
            detail = f"attempt={chunk.attempts} took over from dead worker {previous_worker} redelivered={redelivered}"
        else:
            detail = f"attempt={chunk.attempts} redelivered={redelivered}"
        _log(s, chunk.call_id, actor, "CHUNK_TAKEOVER" if takeover else "CHUNK_CLAIMED", detail, chunk.id)
        return Claim(
            ClaimOutcome.CLAIMED, attempt=chunk.attempts, token=chunk.claim_token, takeover=takeover, **ident
        )


def heartbeat(chunk_id: str, token: str, progress: float) -> bool:
    """Renew the lease. Returns False if we no longer own the chunk (fenced out)."""
    with session_scope() as s:
        res = s.execute(
            update(CallChunk)
            .where(CallChunk.id == chunk_id, CallChunk.claim_token == token, CallChunk.status == ChunkStatus.PROCESSING)
            .values(heartbeat_at=utcnow(), progress=round(progress, 3))
        )
        return res.rowcount == 1


def complete_chunk(claim: Claim, result: dict[str, Any], actor: Actor) -> bool:
    """Store the result, but only if we still hold the lease (fencing)."""
    assert claim.token
    with session_scope() as s:
        now = utcnow()
        res = s.execute(
            update(CallChunk)
            .where(CallChunk.id == claim.chunk_id, CallChunk.claim_token == claim.token, CallChunk.status == ChunkStatus.PROCESSING)
            .values(status=ChunkStatus.COMPLETED, result=result, progress=1.0, completed_at=now, heartbeat_at=now, last_error=None)
        )
        if res.rowcount != 1:
            _log(s, claim.call_id, actor, "LEASE_LOST", "result discarded: another worker owns this chunk", claim.chunk_id)
            return False
        _log(s, claim.call_id, actor, "CHUNK_COMPLETED", f"attempt={claim.attempt}", claim.chunk_id)
        return True


def release_chunk(claim: Claim, error: str, actor: Actor, event: str = "CHUNK_RETRY") -> None:
    """Give the chunk back (PENDING) so the retried delivery can claim it again."""
    assert claim.token
    with session_scope() as s:
        s.execute(
            update(CallChunk)
            .where(CallChunk.id == claim.chunk_id, CallChunk.claim_token == claim.token)
            .values(status=ChunkStatus.PENDING, last_error=error)
        )
        _log(s, claim.call_id, actor, event, error, claim.chunk_id)


def fail_chunk(claim: Claim, error: str, actor: Actor, event: str = "CHUNK_FAILED") -> None:
    """Permanent failure: the chunk and therefore the whole call are FAILED."""
    with session_scope() as s:
        s.execute(update(CallChunk).where(CallChunk.id == claim.chunk_id).values(status=ChunkStatus.FAILED, last_error=error))
        _fail_call(s, claim.call_id, f"chunk {claim.index}: {error}")
        _log(s, claim.call_id, actor, event, error, claim.chunk_id)


# --------------------------------------------------------------------------------------------------
# chord callback
# --------------------------------------------------------------------------------------------------
class FinalizeOutcome(enum.StrEnum):
    COMPLETED = "COMPLETED"
    ALREADY_COMPLETED = "ALREADY_COMPLETED"  # a second chord (e.g. after police recovery) fired: no-op
    CALL_FAILED = "CALL_FAILED"
    INCOMPLETE = "INCOMPLETE"


def finalize_call(call_id: str, actor: Actor, received: int) -> FinalizeOutcome:
    """Aggregate chunk results into the summary and mark the call COMPLETED, exactly once.

    The aggregation reads the *database*, not the list Celery handed to the callback. The chord's
    result list is only the trigger; the DB is the truth and is the same however many times we run.
    """
    with session_scope() as s:
        call = s.get(Call, call_id, with_for_update=True)
        if call is None:
            raise InvalidCall(f"call {call_id} does not exist")
        if call.status == CallStatus.COMPLETED:
            _log(s, call_id, actor, "MERGE_SKIPPED_ALREADY_DONE", f"chord delivered {received} results")
            return FinalizeOutcome.ALREADY_COMPLETED
        if call.status == CallStatus.FAILED:
            _log(s, call_id, actor, "MERGE_SKIPPED_CALL_FAILED", call.error)
            return FinalizeOutcome.CALL_FAILED
        chunks = list(call.chunks)
        if len(chunks) != call.num_chunks or any(c.status != ChunkStatus.COMPLETED for c in chunks):
            done = sum(c.status == ChunkStatus.COMPLETED for c in chunks)
            _log(s, call_id, actor, "MERGE_INCOMPLETE", f"{done}/{call.num_chunks} chunks completed; leaving it to the police")
            return FinalizeOutcome.INCOMPLETE
        call.summary = merge_results([c.result for c in chunks if c.result])
        call.status = CallStatus.COMPLETED
        call.completed_at = utcnow()
        _log(s, call_id, actor, "CALL_COMPLETED", f"chord delivered {received} results; {call.summary['text']}")
        return FinalizeOutcome.COMPLETED


# --------------------------------------------------------------------------------------------------
# task police
# --------------------------------------------------------------------------------------------------
@dataclass(frozen=True)
class CallSnapshot:
    call_id: str
    status: str
    updated_at: datetime
    recovery_count: int
    chunk_activity: list[datetime]  # latest sign of life of each chunk


def snapshot_active_calls() -> list[CallSnapshot]:
    with session_scope() as s:
        calls = s.scalars(
            select(Call).where(Call.status.not_in([str(x) for x in TERMINAL_CALL_STATUSES]))
        ).all()
        snaps = []
        for call in calls:
            activity = [
                _aware(max(t for t in (c.heartbeat_at, c.updated_at) if t is not None)) for c in call.chunks
            ]
            snaps.append(CallSnapshot(call.id, call.status, _aware(call.updated_at), call.recovery_count, activity))
        return snaps


@dataclass(frozen=True)
class RecoveryPlan:
    call_id: str
    status_before: str
    needs_ingest: bool  # no chunk rows yet: the original ingest message was lost
    incomplete_chunk_ids: list[str]
    attempt_number: int


def begin_recovery(call_id: str, actor: Actor, reason: str) -> RecoveryPlan | None:
    with session_scope() as s:
        call = s.get(Call, call_id, with_for_update=True)
        if call is None or call.status in TERMINAL_CALL_STATUSES:
            return None
        call.recovery_count += 1
        call.updated_at = utcnow()  # restart the stall clock so the next sweep leaves it alone
        incomplete = [c.id for c in call.chunks if c.status not in (ChunkStatus.COMPLETED, ChunkStatus.FAILED)]
        _log(s, call_id, actor, "POLICE_RECOVERY", f"#{call.recovery_count}: {reason}; re-dispatching {len(incomplete)} chunk(s)")
        return RecoveryPlan(call_id, call.status, not call.chunks, incomplete, call.recovery_count)
