"""The settings actually in effect on live workers, fetched through Celery's own control channel.

Only settings the state table references are returned (so the UI can show "task_acks_late = True" next
to the state it affects), which also keeps URLs and credentials in the full config out of the API.

Limit worth knowing: this is the *global* value. A per-task override (`@task(acks_late=...)`) lives on
the Task object and is not visible over the wire.
"""
from __future__ import annotations

import time
from typing import Any

from celery_viz.states import load_states


def referenced_settings() -> set[str]:
    return {s["name"] for info in load_states().states.values() for s in info.settings}


def collect(conf_by_worker: dict[str, dict[str, Any]], stats_by_worker: dict[str, dict[str, Any]], wanted: set[str]) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    for name in sorted(wanted):
        values = {host: conf[name] for host, conf in conf_by_worker.items() if name in conf}
        if not values:
            continue
        distinct = {repr(v) for v in values.values()}
        first = next(iter(values.values()))
        settings[name] = {"value": first, "workers": len(values), "mixed": len(distinct) > 1}
    workers = {}
    for host, stats in stats_by_worker.items():
        pool = stats.get("pool") or {}
        workers[host] = {"prefetch_count": stats.get("prefetch_count"), "concurrency": pool.get("max-concurrency")}
    return {"settings": settings, "workers": workers}


class RemoteConfig:
    """Caches inspect results: broadcasting to every worker is not free, and the answer rarely changes."""

    def __init__(self, app: Any, ttl: float = 20.0, timeout: float = 2.0):
        self.app, self.ttl, self.timeout = app, ttl, timeout
        self._cached: dict[str, Any] | None = None
        self._at = 0.0

    def get(self) -> dict[str, Any]:
        if self._cached is not None and time.monotonic() - self._at < self.ttl:
            return self._cached
        try:
            inspector = self.app.control.inspect(timeout=self.timeout)
            result = collect(inspector.conf() or {}, inspector.stats() or {}, referenced_settings())
            result["error"] = None
        except Exception as exc:  # the broker or workers may be down; the UI degrades, it does not break
            result = {"settings": {}, "workers": {}, "error": f"{type(exc).__name__}: {exc}"[:200]}
        self._cached, self._at = result, time.monotonic()
        return result
