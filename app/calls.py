"""Starting a call pipeline: one implementation shared by the HTTP API and the visualizer's scenario catalog."""
from __future__ import annotations

from app.celery_app import celery_app
from database import Call, session_scope


class PublishFailed(Exception):
    """The call row was saved but the ingest message could not be published (the police will recover it)."""

    def __init__(self, call_id: str, cause: Exception):
        super().__init__(f"call {call_id} saved but could not be queued: {cause}")
        self.call_id = call_id


def start_call(title: str, num_chunks: int, faults: dict[str, str] | None = None) -> str:
    with session_scope() as s:
        call = Call(title=title, num_chunks=num_chunks, faults=dict(faults or {}))
        s.add(call)
        s.flush()
        call_id = call.id
    # Publish only after the row is committed so a worker can never see a call that doesn't exist yet.
    try:
        celery_app.send_task("tasks.ingest_call", args=[call_id])
    except Exception as exc:
        raise PublishFailed(call_id, exc) from exc
    return call_id
