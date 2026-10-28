"""FastAPI entrypoint: starts pipelines and exposes their durable state.

The API is deliberately thin. It is a *producer*: it writes the call row, publishes one message
(`send_task` by name, so it never imports task code), and reads state back from PostgreSQL.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Literal

import httpx2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select

from app.calls import PublishFailed, start_call
from app.celery_app import celery_app
from app.logging_config import configure_logging
from config import settings
from database import Call, CallChunk, TaskExecutionLog, session_scope, utcnow

configure_logging(settings.log_level)
log = logging.getLogger("api")

app = FastAPI(title="Call Pipeline", description="Celery learning project: a call-intelligence pipeline.")

Fault = Literal["transient", "permanent", "crash", "hang", "bug", "slow"]


class CreateCall(BaseModel):
    title: str = "Mock support call"
    num_chunks: int = Field(5, ge=1, le=50)
    faults: dict[int, Fault] = Field(
        default_factory=dict,
        description='Inject a failure into chunk N, e.g. {"2": "crash"}. See tasks/pipeline.py for what each does.',
    )


def _iso(dt: datetime | None) -> str | None:
    return dt.isoformat() if dt else None


def _chunk_dict(c: CallChunk, now: datetime) -> dict[str, Any]:
    return {
        "index": c.index,
        "id": c.id,
        "status": c.status,
        "attempts": c.attempts,
        "worker": c.worker,
        "progress": c.progress,
        "fault": c.fault,
        "heartbeat_age_s": round((now - c.heartbeat_at).total_seconds(), 1) if c.heartbeat_at else None,
        "started_at": _iso(c.started_at),
        "completed_at": _iso(c.completed_at),
        "last_error": c.last_error,
    }


def _call_dict(call: Call, with_chunks: bool = True) -> dict[str, Any]:
    now = utcnow()
    data: dict[str, Any] = {
        "id": call.id,
        "title": call.title,
        "status": call.status,
        "num_chunks": call.num_chunks,
        "recovery_count": call.recovery_count,
        "error": call.error,
        "summary": call.summary,
        "created_at": _iso(call.created_at),
        "completed_at": _iso(call.completed_at),
    }
    if with_chunks:
        data["chunks"] = [_chunk_dict(c, now) for c in call.chunks]
    return data


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/config")
def reliability_config() -> dict[str, Any]:
    """The reliability settings this deployment is running with (workers get the same environment)."""
    c = celery_app.conf
    return {
        "reliability_mode": settings.reliability_mode,
        "task_acks_late": c.task_acks_late,
        "task_reject_on_worker_lost": c.task_reject_on_worker_lost,
        "worker_prefetch_multiplier": c.worker_prefetch_multiplier,
        "worker_cancel_long_running_tasks_on_connection_loss": c.worker_cancel_long_running_tasks_on_connection_loss,
        "task_soft_time_limit": c.task_soft_time_limit,
        "task_time_limit": c.task_time_limit,
        "lease_seconds": settings.lease_seconds,
        "max_attempts": settings.max_attempts,
        "police_interval_seconds": settings.police_interval_seconds,
        "stall_threshold_seconds": settings.stall_threshold_seconds,
    }


@app.post("/calls", status_code=202)
def create_call(body: CreateCall) -> dict[str, Any]:
    try:
        call_id = start_call(body.title, body.num_chunks, {str(k): v for k, v in body.faults.items()})
    except PublishFailed as exc:  # broker down: the row exists as PENDING and the police will recover it
        log.exception("could not publish ingest_call", extra={"call_id": exc.call_id})
        raise HTTPException(503, str(exc)) from exc
    log.info("call accepted", extra={"call_id": call_id, "chunks": body.num_chunks})
    return {"call_id": call_id, "status": "PENDING"}


@app.get("/calls")
def list_calls(limit: int = 20) -> list[dict[str, Any]]:
    with session_scope() as s:
        calls = s.scalars(select(Call).order_by(Call.created_at.desc()).limit(min(limit, 100))).all()
        return [_call_dict(c, with_chunks=False) for c in calls]


@app.get("/calls/{call_id}")
def get_call(call_id: str) -> dict[str, Any]:
    with session_scope() as s:
        call = s.get(Call, call_id)
        if call is None:
            raise HTTPException(404, "call not found")
        return _call_dict(call)


@app.get("/calls/{call_id}/events")
def get_events(call_id: str) -> list[dict[str, Any]]:
    """The audit trail: every claim, takeover, retry, skip and completion, in order."""
    with session_scope() as s:
        rows = s.scalars(
            select(TaskExecutionLog).where(TaskExecutionLog.call_id == call_id).order_by(TaskExecutionLog.id)
        ).all()
        chunk_index = {c.id: c.index for c in s.scalars(select(CallChunk).where(CallChunk.call_id == call_id))}
        return [
            {
                "at": _iso(r.created_at),
                "event": r.event,
                "task": r.task_name,
                "chunk_index": chunk_index.get(r.chunk_id) if r.chunk_id else None,
                "worker": r.worker,
                "celery_task_id": r.celery_task_id,
                "detail": r.detail,
            }
            for r in rows
        ]


@app.get("/dead-letters")
def dead_letters(count: int = 20) -> dict[str, Any]:
    """Peek (without consuming) at messages parked in the dead_letter queue."""
    try:
        resp = httpx2.post(
            f"{settings.rabbitmq_api_url}/queues/%2F/dead_letter/get",
            auth=(settings.rabbitmq_user, settings.rabbitmq_password),
            json={"count": count, "ackmode": "ack_requeue_true", "encoding": "auto", "truncate": 2000},
            timeout=5,
        )
        resp.raise_for_status()
    except httpx2.HTTPError as exc:
        raise HTTPException(502, f"RabbitMQ management API unavailable: {exc}") from exc
    messages = []
    for m in resp.json():
        headers = m.get("properties", {}).get("headers", {})
        death = (headers.get("x-death") or [{}])[0]
        messages.append(
            {
                "task": headers.get("task"),
                "args": headers.get("argsrepr"),
                "task_id": headers.get("id"),
                "died_in_queue": death.get("queue"),
                "reason": death.get("reason"),
                "times_dead_lettered": death.get("count"),
            }
        )
    return {"count": len(messages), "messages": messages}
