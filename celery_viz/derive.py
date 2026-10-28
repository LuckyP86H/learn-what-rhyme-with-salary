"""Read-time views: everything that depends on 'now' or needs inference lives here, never in the store.

Keeping it separate matters: the store only records facts that arrived as events, while this module
produces the *guesses* (worker lost, suspected hung, inferred fan-in) and labels them as guesses.
"""
from __future__ import annotations

from typing import Any

from celery_viz.model import Run, Task, Worker
from celery_viz.states import load_states
from celery_viz.store import RunStore

_WAITING_FOR_WORKER = ("RECEIVED", "STARTED", "WAITING_ETA")


def worker_is_lost(worker: Worker | None, now: float, store: RunStore) -> bool:
    """Silent for too long. A worker that announced worker-offline is NOT 'lost': it is shutting down
    (see worker_is_draining), and Celery's warm shutdown still lets in-flight tasks finish."""
    if worker is None or worker.status == "offline":
        return False
    cfg = store.config
    limit = max(cfg.min_lost_after_seconds, cfg.lost_after_heartbeats * (worker.freq or 5.0))
    return (now - worker.last_seen_local) > limit


def worker_is_draining(worker: Worker | None, now: float, store: RunStore) -> bool:
    return bool(worker and worker.status == "offline" and worker.offline_since_local is not None
                and (now - worker.offline_since_local) <= store.config.drain_grace_seconds)


def silent_ended_keys(store: RunStore) -> set[str]:
    """Tasks we believe are STARTED but whose worker reports executing fewer tasks than that.

    Celery emits no terminal event when a task raises Ignore (self.replace() does this) or when the
    event is lost, so the task would look 'running' forever. A worker's heartbeat carries `active`, the
    number of tasks it is executing; if that is lower than the number of STARTED tasks we hold for it
    (counting only tasks that began before the heartbeat that REPORTED `active`), the oldest ones must have ended.
    """
    ended: set[str] = set()
    for host, keys in store.started_on.items():
        worker = store.workers.get(host)
        if worker is None or worker.active is None or not keys:
            continue
        eligible = sorted(
            (store.tasks[k] for k in keys if k in store.tasks and store.tasks[k].started_local is not None
             and store.tasks[k].started_local < worker.active_seen_local - 0.5),
            key=lambda t: t.started_local or 0.0,
        )
        excess = len(eligible) - worker.active
        if excess > 0:
            ended.update(t.key for t in eligible[:excess])
    return ended


def effective_state(task: Task, store: RunStore, now: float, silent: set[str] | None = None) -> tuple[str, str | None]:
    """The state to show *now*: the stored state, upgraded to a derived state when the clock says so."""
    state = task.state
    if state == "STARTED" and task.key in (silent_ended_keys(store) if silent is None else silent):
        return "ENDED_SILENTLY", f"{task.worker} reports it is no longer executing this, but no terminal event arrived"
    worker = store.workers.get(task.worker or "")
    if state == "RECEIVED" and worker is not None and worker.status == "offline":
        # A warm shutdown only finishes tasks that are RUNNING. A merely reserved task is never started and its
        # unacknowledged message goes back to the queue, so showing it as "draining" would be wrong.
        return "QUEUED", f"{task.worker} announced shutdown before starting this; unacknowledged messages go back on the queue"
    if state == "STARTED" and worker is not None and worker.status == "offline":
        if worker_is_draining(worker, now, store):
            return "DRAINING", f"{task.worker} announced shutdown; a warm shutdown lets in-flight tasks finish"
        return "WORKER_LOST", f"{task.worker} went offline and this task never finished"
    if state in ("RECEIVED", "STARTED") and worker_is_lost(worker, now, store):
        return "WORKER_LOST", f"{task.worker} has not sent a heartbeat for a while (it may have been killed)"
    if state == "RECEIVED" and task.eta and task.eta > now:
        return "WAITING_ETA", f"will run in {task.eta - now:.0f}s"
    hung = store.config.hung_after_seconds
    if state == "STARTED" and hung and task.started_local and (now - task.started_local) > hung:
        return "SUSPECTED_HUNG", f"running for {now - task.started_local:.0f}s (threshold {hung:.0f}s)"
    return state, None


def _fan_in_edges(run_tasks: list[Task]) -> list[dict[str, str]]:
    """Chord join edges. Celery gives the callback only ONE parent (the last header task to finish),
    so we infer the rest: finished siblings of that parent, same name, same spawner.

    Indexed once so the cost stays linear: a run can hold thousands of parallel tasks and this runs
    every second while the store lock is held.
    """
    by_key = {t.key: t for t in run_tasks}
    siblings: dict[tuple[str | None, str | None], list[Task]] = {}
    for t in run_tasks:
        siblings.setdefault((t.parent_key, t.name), []).append(t)
    edges: list[dict[str, str]] = []
    for callback in run_tasks:
        parent = by_key.get(callback.parent_key) if callback.parent_key else None
        if parent is None or parent.name == callback.name:
            continue
        for sib in siblings.get((parent.parent_key, parent.name), ()):
            if sib.key != parent.key and sib.ended_at is not None and callback.sent_at is not None and sib.ended_at <= callback.sent_at + 1.0:
                edges.append({"from": sib.key, "to": callback.key, "kind": "inferred_fanin"})
    return edges


def task_view(task: Task, store: RunStore, now: float, silent: set[str] | None = None) -> dict[str, Any]:
    table = load_states()
    state, note = effective_state(task, store, now, silent)
    info = table.get(state)
    return {
        "key": task.key,
        "uuid": task.uuid,
        "name": task.name,
        "state": state if state in table.states else "UNKNOWN",
        "raw_state": task.state,  # what the events said; "state" may be a derived upgrade of it
        "station": info.station,
        "phase": info.phase,
        "note": note,
        "queue": task.queue,
        "worker": task.worker,
        "retries": task.retries,
        "eta": task.eta,
        "redelivered": task.redelivered,
        "duplicates": task.duplicates,
        "runtime": task.runtime,
        "exception": task.exception,
        "sent_at": task.sent_at,
        "started_at": task.started_at,
        "ended_at": task.ended_at,
        "parent_key": task.parent_key,
        "replaced_from": task.replaced_from,
        "timeline": [{"ts": s.ts, "state": s.state, "worker": s.worker, "note": s.note, "transition": s.transition} for s in task.timeline],
    }


def run_status(views: list[dict[str, Any]]) -> str:
    phases = {v["phase"] for v in views}
    if "attention" in phases:
        return "ATTENTION"
    if "active" in phases:
        return "RUNNING"
    if "done_bad" in phases:
        return "FAILED"
    return "COMPLETED"


def run_view(store: RunStore, run: Run, now: float | None = None) -> dict[str, Any]:
    now = store.now() if now is None else now
    tasks = [store.tasks[k] for k in run.task_keys if k in store.tasks]
    silent = silent_ended_keys(store)
    views = [task_view(t, store, now, silent) for t in tasks]
    edges = [{"from": t.parent_key, "to": t.key, "kind": "spawned"} for t in tasks if t.parent_key]
    edges += [{"from": t.replaced_from, "to": t.key, "kind": "replaced"} for t in tasks if t.replaced_from]
    edges += _fan_in_edges(tasks)
    counts: dict[str, int] = {}
    for v in views:
        counts[v["state"]] = counts.get(v["state"], 0) + 1
    names = [t.name for t in tasks if t.name]
    return {
        "id": run.id,
        "title": names[0] if names else run.id[:8],
        "status": run_status(views),
        "has_failures": any(v["phase"] == "done_bad" for v in views),
        "started_at": run.first_ts,
        "updated_at": run.updated_local,
        "counts": counts,
        "tasks": views,
        "edges": edges,
    }


def run_summary(store: RunStore, run: Run, now: float | None = None) -> dict[str, Any]:
    view = run_view(store, run, now)
    return {k: view[k] for k in ("id", "title", "status", "has_failures", "started_at", "updated_at", "counts")} | {"task_count": len(view["tasks"])}


def workers_view(store: RunStore, now: float | None = None) -> list[dict[str, Any]]:
    now = store.now() if now is None else now
    return [
        {
            "hostname": w.hostname,
            "status": "lost" if worker_is_lost(w, now, store) else w.status,
            "software": w.software,
            "freq": w.freq,
            "silent_for": round(now - w.last_seen_local, 1),
            "active": w.active,
            "processed": w.processed,
        }
        for w in sorted(store.workers.values(), key=lambda w: w.hostname)
    ]
