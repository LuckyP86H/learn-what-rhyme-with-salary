"""RunStore: folds a stream of Celery events into runs of tasks.

Design rules (each one comes from something observed in recorded events, see tests/viz/):
  * A task id is NOT a unique identity: self.replace() reuses it under a new name. Tasks are keyed by
    (uuid, generation).
  * Events are lossy and can interleave out of order across hosts. Terminal states are absorbing;
    late duplicates are recorded as notes, never as state regressions.
  * task-started / -succeeded events carry only a uuid; name, parent and root are joined in from
    earlier events. Unknown uuids get a placeholder, so a monitor that joins mid-flight still works.
  * The store never calls the broker; it only folds events. Time-dependent states (worker lost, ETA
    waiting, suspected hung) are computed at read time in derive.py.
"""
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any

from celery_viz.config import VizConfig
from celery_viz.model import Run, Step, Task, Worker
from celery_viz.states import load_states


def parse_eta(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except ValueError:
        return None


class RunStore:
    def __init__(self, config: VizConfig | None = None, live: bool = False):
        self.config = config or VizConfig()
        self.live = live  # live: "now" is the wall clock; replay: "now" is the newest event seen
        self.lock = threading.RLock()
        self.tasks: dict[str, Task] = {}
        self.by_uuid: dict[str, list[str]] = {}
        self.runs: OrderedDict[str, Run] = OrderedDict()
        self.workers: dict[str, Worker] = {}
        self.started_on: dict[str, set[str]] = {}  # worker -> keys of tasks we believe are executing
        self.seq = 0
        self._virtual_now = 0.0
        self._terminal = {s.key for s in load_states().states.values() if s.phase in ("done_ok", "done_bad")}
        self.listeners: list[Any] = []  # callables(run_id | None); invoked after each change

    # ------------------------------------------------------------------ time
    def now(self) -> float:
        return time.time() if self.live else self._virtual_now

    # ------------------------------------------------------------------ ingest
    def apply(self, event: dict[str, Any]) -> None:
        etype = event.get("type", "")
        with self.lock:
            local = float(event.get("local_received") or event.get("timestamp") or 0.0)
            self._virtual_now = max(self._virtual_now, local)
            changed: str | None = None
            if etype.startswith("worker-"):
                self._worker_event(etype, event, local)
            elif etype.startswith("task-") and event.get("uuid"):
                handler = getattr(self, "_on_" + etype.replace("-", "_"), None)
                if handler:
                    changed = handler(event, local)
            else:
                return
            self.seq += 1
        for listener in list(self.listeners):
            listener(changed)

    # ------------------------------------------------------------------ helpers
    def _current(self, uuid: str) -> Task | None:
        keys = self.by_uuid.get(uuid)
        return self.tasks[keys[-1]] if keys else None

    def _run(self, run_id: str, ts: float, local: float) -> Run:
        run = self.runs.get(run_id)
        if run is None:
            run = self.runs[run_id] = Run(run_id, first_ts=ts)
            self._evict()
        run.updated_local = local
        self.runs.move_to_end(run_id)
        return run

    def _evict(self) -> None:
        while len(self.runs) > self.config.max_runs:
            _, old = self.runs.popitem(last=False)
            for key in old.task_keys:
                task = self.tasks.pop(key, None)
                if task and task.uuid in self.by_uuid:
                    self.by_uuid[task.uuid] = [k for k in self.by_uuid[task.uuid] if k != key]
                    if not self.by_uuid[task.uuid]:
                        del self.by_uuid[task.uuid]

    def _new_task(self, e: dict[str, Any], local: float, name: str | None, replaced: Task | None = None) -> Task:
        uuid = e["uuid"]
        gen = len(self.by_uuid.get(uuid, []))
        run_id = e.get("root_id") or (replaced.run_id if replaced else uuid)
        parent_uuid = e.get("parent_id")
        parent = self._current(parent_uuid) if parent_uuid else None
        task = Task(uuid=uuid, gen=gen, name=name, run_id=run_id, parent_key=parent.key if parent else None,
                    replaced_from=replaced.key if replaced else None, queue=e.get("queue"))
        run = self._run(run_id, e.get("timestamp", local), local)
        if len(run.task_keys) >= self.config.max_tasks_per_run:
            raise OverflowError("run exceeds max_tasks_per_run")
        run.task_keys.append(task.key)
        self.tasks[task.key] = task
        self.by_uuid.setdefault(uuid, []).append(task.key)
        return task

    def _get_or_create(self, e: dict[str, Any], local: float) -> Task:
        task = self._current(e["uuid"])
        name = e.get("name")
        if task is None:
            return self._new_task(e, local, name)
        if name and task.name and name != task.name:
            # Same id, different task name: the original called self.replace().
            self._mark(task, "REPLACED", e.get("timestamp", local), note=f"replaced by {name}")
            task.ended_at = e.get("timestamp", local)
            return self._new_task(e, local, name, replaced=task)
        if name and not task.name:
            task.name = name
        root = e.get("root_id")
        if root and task.run_id != root:
            self._rehome(task, root)
        return task

    def _rehome(self, task: Task, run_id: str) -> None:
        old = self.runs.get(task.run_id)
        if old and task.key in old.task_keys:
            old.task_keys.remove(task.key)
            if not old.task_keys:
                self.runs.pop(old.id, None)
        task.run_id = run_id
        self._run(run_id, task.sent_at or 0.0, self._virtual_now).task_keys.append(task.key)

    def _mark(self, task: Task, state: str, ts: float, worker: str | None = None, note: str | None = None) -> None:
        if task.state == "STARTED" and task.worker:
            self.started_on.get(task.worker, set()).discard(task.key)
        task.state = state
        if worker:
            task.worker = worker
        if state == "STARTED" and task.worker:
            self.started_on.setdefault(task.worker, set()).add(task.key)
        task.timeline.append(Step(ts=ts, state=state, worker=worker or task.worker, note=note))

    def _note(self, task: Task, ts: float, note: str) -> None:
        task.timeline.append(Step(ts=ts, state=task.state, worker=task.worker, note=note, transition=False))

    def _is_terminal(self, task: Task) -> bool:
        return task.state in self._terminal

    # ------------------------------------------------------------------ task events
    def _on_task_sent(self, e: dict[str, Any], local: float) -> str:
        ts = e.get("timestamp", local)
        task = self._get_or_create(e, local)
        retries = int(e.get("retries") or 0)
        if not task.timeline:  # first sight of this identity
            task.sent_at, task.retries, task.queue = ts, retries, e.get("queue")
            task.eta = parse_eta(e.get("eta"))
            self._mark(task, "QUEUED", ts, note=f"published to queue {e.get('queue')!r}")
        elif retries > task.retries:  # a retry: a NEW message for the same id
            task.retries = retries
            task.eta = parse_eta(e.get("eta"))
            task.queue = e.get("queue") or task.queue
            if task.delivery_retries is not None and task.delivery_retries >= retries:
                self._note(task, ts, f"retry #{retries} published (already received)")
            else:
                self._mark(task, "QUEUED", ts, note=f"retry #{retries} published, ETA {e.get('eta')}")
        return task.run_id

    def _on_task_received(self, e: dict[str, Any], local: float) -> str:
        ts = e.get("timestamp", local)
        host = e.get("hostname")
        task = self._get_or_create(e, local)
        retries = int(e.get("retries") or 0)
        if self._is_terminal(task):
            task.duplicates += 1
            self._note(task, ts, f"duplicate delivery to {host} after completion (an idempotent task skips it)")
            return task.run_id
        if task.state in ("RECEIVED", "STARTED") and task.delivery_retries == retries and task.worker:
            task.redelivered += 1
            self._note(task, ts, f"redelivered: the delivery held by {task.worker} was never finished")
        if retries > task.retries:
            task.retries = retries
        task.delivery_retries = retries
        task.eta = parse_eta(e.get("eta")) or task.eta
        task.started_at = task.started_local = None
        self._mark(task, "RECEIVED", ts, worker=host)
        return task.run_id

    def _on_task_started(self, e: dict[str, Any], local: float) -> str:
        ts = e.get("timestamp", local)
        task = self._get_or_create(e, local)
        if self._is_terminal(task):
            task.duplicates += 1
            self._note(task, ts, f"duplicate execution on {e.get('hostname')} after completion")
            return task.run_id
        task.started_at, task.started_local = ts, local
        self._mark(task, "STARTED", ts, worker=e.get("hostname"))
        return task.run_id

    def _on_task_succeeded(self, e: dict[str, Any], local: float) -> str:
        task = self._get_or_create(e, local)
        ts = e.get("timestamp", local)
        if self._is_terminal(task):
            task.duplicates += 1
            self._note(task, ts, f"duplicate completion on {e.get('hostname')}")
            return task.run_id
        task.runtime, task.ended_at = e.get("runtime"), ts
        self._mark(task, "SUCCESS", ts, worker=e.get("hostname"), note=f"ran {e['runtime']:.2f}s" if e.get("runtime") is not None else None)
        return task.run_id

    def _on_task_failed(self, e: dict[str, Any], local: float) -> str:
        task = self._get_or_create(e, local)
        ts = e.get("timestamp", local)
        if not self._is_terminal(task):
            task.exception, task.ended_at = e.get("exception"), ts
            self._mark(task, "FAILURE", ts, worker=e.get("hostname"), note=e.get("exception"))
        return task.run_id

    def _on_task_retried(self, e: dict[str, Any], local: float) -> str:
        task = self._get_or_create(e, local)
        ts = e.get("timestamp", local)
        task.retried_events += 1
        task.exception = e.get("exception") or task.exception
        if task.retried_events > task.retries and not self._is_terminal(task):
            self._mark(task, "RETRY", ts, worker=e.get("hostname"), note=e.get("exception"))
        else:  # the republished message was already seen; just attach the reason
            self._note(task, ts, f"retry reason: {e.get('exception')}")
        return task.run_id

    def _on_task_rejected(self, e: dict[str, Any], local: float) -> str:
        task = self._get_or_create(e, local)
        ts = e.get("timestamp", local)
        if self._is_terminal(task):
            return task.run_id
        if e.get("requeue"):
            self._mark(task, "QUEUED", ts, note="rejected with requeue=True: back on the queue")
        else:
            task.ended_at = ts
            self._mark(task, "REJECTED", ts, worker=e.get("hostname"), note="rejected with requeue=False (dead-lettered if the queue has a DLX)")
        return task.run_id

    def _on_task_revoked(self, e: dict[str, Any], local: float) -> str:
        task = self._get_or_create(e, local)
        ts = e.get("timestamp", local)
        if not self._is_terminal(task):
            task.ended_at = ts
            why = "expired" if e.get("expired") else "terminated" if e.get("terminated") else "revoked"
            self._mark(task, "REVOKED", ts, worker=e.get("hostname"), note=why)
        return task.run_id

    # ------------------------------------------------------------------ worker events
    def _worker_event(self, etype: str, e: dict[str, Any], local: float) -> None:
        host = e.get("hostname")
        if not host:
            return
        w = self.workers.setdefault(host, Worker(hostname=host))
        w.last_seen_local = max(w.last_seen_local, local)
        w.freq = e.get("freq", w.freq)
        w.software = f"{e['sw_ident']} {e['sw_ver']}" if e.get("sw_ver") else w.software
        w.active, w.processed = e.get("active", w.active), e.get("processed", w.processed)
        if etype == "worker-offline":
            w.status = "offline"
        elif etype in ("worker-online", "worker-heartbeat"):
            w.status = "online"
