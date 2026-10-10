"""Plain data holders. Behaviour lives in store.py (state changes) and derive.py (read-time views)."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Step:
    """One entry in a task's timeline: where the ball was, and when."""

    ts: float
    state: str
    worker: str | None = None
    note: str | None = None
    transition: bool = True  # False: an annotation on the current station (the ball does not move)


@dataclass
class Task:
    """One *identity* of a task: (uuid, gen). gen increases when the same id is reused under a new
    name, which is what self.replace() does, so a uuid alone is not unique."""

    uuid: str
    gen: int
    name: str | None
    run_id: str
    parent_key: str | None = None
    replaced_from: str | None = None
    queue: str | None = None
    state: str = "QUEUED"
    worker: str | None = None
    retries: int = 0
    delivery_retries: int | None = None  # `retries` value of the delivery currently held by a worker
    retried_events: int = 0
    eta: float | None = None
    sent_at: float | None = None
    started_at: float | None = None
    started_local: float | None = None
    ended_at: float | None = None
    runtime: float | None = None
    exception: str | None = None
    redelivered: int = 0
    duplicates: int = 0
    timeline: list[Step] = field(default_factory=list)

    @property
    def key(self) -> str:
        return f"{self.uuid}:{self.gen}"


@dataclass
class Worker:
    hostname: str
    status: str = "online"  # "online" | "offline" (graceful); a SIGKILLed worker stays "online" and just goes silent
    freq: float | None = None
    last_seen_local: float = 0.0  # any worker event: proves the worker is alive
    offline_since_local: float | None = None  # set by worker-offline; cleared if it comes back
    active_seen_local: float = 0.0  # when `active` was last REPORTED (short heartbeats omit it)
    software: str | None = None
    active: int | None = None
    processed: int | None = None


@dataclass
class Run:
    id: str
    task_keys: list[str] = field(default_factory=list)
    first_ts: float = 0.0
    updated_local: float = 0.0
