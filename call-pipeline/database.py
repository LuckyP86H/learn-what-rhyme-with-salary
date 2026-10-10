"""SQLAlchemy models + session handling.

PostgreSQL is the *durable source of truth* for the workflow. Celery's broker and result backend are
treated as transport: they may lose or duplicate messages, so nothing important lives only there.
"""
from __future__ import annotations

import enum
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Index, Integer, String, Text, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

from config import settings


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def new_id() -> str:
    return str(uuid.uuid4())


class CallStatus(enum.StrEnum):
    PENDING = "PENDING"  # row created by the API, ingest not run yet
    INGESTED = "INGESTED"  # metadata validated, chunk rows exist
    PROCESSING = "PROCESSING"  # at least one chunk has been claimed
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ChunkStatus(enum.StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


TERMINAL_CALL_STATUSES = (CallStatus.COMPLETED, CallStatus.FAILED)


class Base(DeclarativeBase):
    pass


class Call(Base):
    __tablename__ = "calls"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    title: Mapped[str] = mapped_column(String(200))
    num_chunks: Mapped[int] = mapped_column(Integer)
    # {"2": "crash"}: chunk index -> fault to inject (see tasks/pipeline.py). Lets you rehearse failures.
    faults: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(20), default=CallStatus.PENDING, index=True)
    recovery_count: Mapped[int] = mapped_column(Integer, default=0)
    summary: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    chunks: Mapped[list[CallChunk]] = relationship(back_populates="call", order_by="CallChunk.index")


class CallChunk(Base):
    __tablename__ = "call_chunks"
    __table_args__ = (UniqueConstraint("call_id", "index", name="uq_chunk_call_index"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    call_id: Mapped[str] = mapped_column(ForeignKey("calls.id"), index=True)
    index: Mapped[int] = mapped_column(Integer)
    audio_uri: Mapped[str] = mapped_column(String(300))
    fault: Mapped[str | None] = mapped_column(String(20), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default=ChunkStatus.PENDING)
    # Counts *executions*, including ones that crashed or were redelivered. Celery's own
    # `request.retries` cannot see those; this is what stops a poison pill looping forever.
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    # Lease + fencing token: whoever holds the token owns the chunk. A zombie worker that wakes up
    # after someone else took over fails its token check instead of overwriting newer results.
    claim_token: Mapped[str | None] = mapped_column(String(32), nullable=True)
    worker: Mapped[str | None] = mapped_column(String(120), nullable=True)
    progress: Mapped[float] = mapped_column(Float, default=0.0)
    heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    call: Mapped[Call] = relationship(back_populates="chunks")


class TaskExecutionLog(Base):
    """Append-only audit trail: what every task did, on which worker, and why.

    The chaos scripts read this to answer "was the chunk re-queued or dropped?".
    """

    __tablename__ = "task_execution_log"
    __table_args__ = (Index("ix_log_call_created", "call_id", "created_at"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    call_id: Mapped[str] = mapped_column(String(36))
    chunk_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    task_name: Mapped[str] = mapped_column(String(80))
    celery_task_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    worker: Mapped[str | None] = mapped_column(String(120), nullable=True)
    event: Mapped[str] = mapped_column(String(40))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


# pool_pre_ping: transparently replace connections that PostgreSQL closed while a worker slept.
engine = create_engine(settings.database_url, pool_pre_ping=True, pool_size=5, max_overflow=5)
SessionLocal = sessionmaker(engine, expire_on_commit=False)


@contextmanager
def session_scope() -> Iterator[Any]:
    """One short transaction: commit on success, roll back on any error."""
    with SessionLocal() as session:
        try:
            yield session
            session.commit()
        except BaseException:
            session.rollback()
            raise


def init_db() -> None:
    Base.metadata.create_all(engine)
