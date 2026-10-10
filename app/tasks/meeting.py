"""Plain Celery tasks about a meeting: the "typical project" shape, for the visualizer's scenarios.

Unlike the call pipeline these have NO lease/heartbeat/idempotency machinery. They are ordinary tasks,
so what you see in the visualizer is Celery's own behaviour (retry with backoff, plain FAILURE, ...).
Everything is simulated: no real audio, no real LLM, no cost, and results carry no personal data.
"""
from __future__ import annotations

import random
import time
from typing import Any

from celery import Task

from app.celery_app import celery_app
from app.errors import TransientError

FAULTS = ("none", "transient", "permanent", "slow")
SLOW_SECONDS = 22.0  # stays under task_soft_time_limit (30 s)


def _work(task: Task, lo: float, hi: float, fault: str | None, seed: str) -> float:
    """Pretend to work for lo..hi seconds (deterministic per seed), then maybe misbehave."""
    seconds = random.Random(seed).uniform(lo, hi)
    time.sleep(SLOW_SECONDS if fault == "slow" else seconds)
    if fault == "transient" and task.request.retries < 2:
        raise TransientError(f"upstream timeout (attempt {task.request.retries + 1})")
    if fault == "permanent":
        raise ValueError("unsupported audio format")  # an ordinary exception -> Celery state FAILURE
    return seconds


_retrying = dict(autoretry_for=(TransientError,), retry_backoff=2, retry_backoff_max=20, retry_jitter=False, max_retries=3)


@celery_app.task(bind=True, name="tasks.meeting_summarize_one", **_retrying)
def summarize_meeting(self: Task, meeting_id: str, fault: str | None = None) -> dict[str, Any]:
    """The single-task scenario."""
    took = _work(self, 3, 6, fault, f"{meeting_id}:summary")
    return {"meeting": meeting_id, "summary_words": 120, "seconds": round(took, 1)}


@celery_app.task(bind=True, name="tasks.meeting_transcribe", **_retrying)
def transcribe(self: Task, meeting_id: str, fault: str | None = None) -> dict[str, Any]:
    took = _work(self, 3, 5, fault, f"{meeting_id}:transcribe")
    return {"meeting": meeting_id, "words": 900, "seconds": round(took, 1)}


@celery_app.task(bind=True, name="tasks.meeting_summarize", **_retrying)
def summarize(self: Task, transcript: dict[str, Any], fault: str | None = None) -> dict[str, Any]:
    took = _work(self, 2, 4, fault, f"{transcript['meeting']}:summarize")
    return {"meeting": transcript["meeting"], "summary_words": 120, "seconds": round(took, 1)}


@celery_app.task(bind=True, name="tasks.meeting_extract_actions", **_retrying)
def extract_actions(self: Task, summary: dict[str, Any], fault: str | None = None) -> dict[str, Any]:
    took = _work(self, 1, 3, fault, f"{summary['meeting']}:actions")
    return {"meeting": summary["meeting"], "action_items": 3, "seconds": round(took, 1)}


@celery_app.task(bind=True, name="tasks.meeting_translate", **_retrying)
def translate(self: Task, meeting_id: str, language: str, fault: str | None = None) -> dict[str, Any]:
    """One of several independent translations run at the same time (a group)."""
    took = _work(self, 3, 7, fault, f"{meeting_id}:{language}")
    return {"meeting": meeting_id, "language": language, "seconds": round(took, 1)}
