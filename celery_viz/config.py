from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class VizConfig:
    # A worker is "lost" once silent for this many of ITS OWN heartbeat periods (the `freq` field in
    # its heartbeat events). Celery's docs say heartbeats arrive every minute; real workers vary
    # (ours send every 5 s), so the threshold is derived per worker instead of hard-coded.
    lost_after_heartbeats: float = 3.0
    min_lost_after_seconds: float = 12.0

    # Celery has no per-task heartbeat, so "hung" is a guess: running longer than this. None = off.
    hung_after_seconds: float | None = None

    # Memory bounds: the store is a ring buffer, never an archive.
    max_runs: int = 500
    max_tasks_per_run: int = 2000
