from collections import Counter

from celery_viz.derive import effective_state, run_view, workers_view
from celery_viz.replay import replay_into
from celery_viz.store import RunStore

from .conftest import build_store, events_of


def runs_by_start(store):
    return sorted(store.runs.values(), key=lambda r: r.first_ts)


def pipeline_runs(store):
    """The four scenario runs (ingest_call roots), in the order they were recorded."""
    out = []
    for run in runs_by_start(store):
        view = run_view(store, run)
        if any(t["name"] == "tasks.ingest_call" for t in view["tasks"]):
            out.append(view)
    return out


# ---- the happy-path chord ---------------------------------------------------------------------
def test_replace_gives_one_task_id_two_identities(celery5):
    happy = pipeline_runs(celery5)[0]
    by_uuid = Counter(t["uuid"] for t in happy["tasks"])
    shared = [u for u, n in by_uuid.items() if n == 2]
    assert len(shared) == 1, "ingest_call and merge_and_finalize share one Celery task id"
    first, second = sorted((t for t in happy["tasks"] if t["uuid"] == shared[0]), key=lambda t: t["key"])
    assert (first["name"], first["state"]) == ("tasks.ingest_call", "REPLACED")
    assert (second["name"], second["state"]) == ("tasks.merge_and_finalize", "SUCCESS")


def test_ingest_is_never_left_looking_like_it_is_still_running(celery5):
    # ingest_call emits NO terminal event (self.replace raises Ignore). If the chord's callback ran we
    # infer REPLACED from the re-published id; if the chord is blocked (a chunk was rejected) there is no
    # callback, so we fall back on the worker's heartbeat 'active' count and report ENDED_SILENTLY.
    for run in pipeline_runs(celery5):
        for t in (t for t in run["tasks"] if t["name"] == "tasks.ingest_call"):
            assert t["state"] in ("REPLACED", "ENDED_SILENTLY"), (run["id"], t["state"])


def test_blocked_chord_parent_is_explained_not_stuck(celery5):
    blocked = pipeline_runs(celery5)[2]
    ingest = next(t for t in blocked["tasks"] if t["name"] == "tasks.ingest_call")
    assert ingest["raw_state"] == "STARTED", "the stored state is what Celery reported: it never said it finished"
    assert ingest["state"] == "ENDED_SILENTLY"
    assert not any(t["name"] == "tasks.merge_and_finalize" for t in blocked["tasks"]), "the callback never ran"


def test_chord_headers_hang_off_the_spawner_and_the_run_completes(celery5):
    happy = pipeline_runs(celery5)[0]
    ingest = next(t for t in happy["tasks"] if t["name"] == "tasks.ingest_call")
    chunks = [t for t in happy["tasks"] if t["name"] == "tasks.analyze_chunk"]
    assert len(chunks) == 3 and all(t["state"] == "SUCCESS" for t in chunks)
    assert all(t["parent_key"] == ingest["key"] for t in chunks)
    assert happy["status"] == "COMPLETED"


def test_fan_in_edges_are_inferred_and_labelled_as_inferred(celery5):
    happy = pipeline_runs(celery5)[0]
    merge = next(t for t in happy["tasks"] if t["name"] == "tasks.merge_and_finalize")
    chunk_keys = {t["key"] for t in happy["tasks"] if t["name"] == "tasks.analyze_chunk"}
    into_merge = [e for e in happy["edges"] if e["to"] == merge["key"] and e["from"] in chunk_keys]
    # Celery reports ONE parent (the last chunk to finish); the other two joins are our inference.
    assert {e["kind"] for e in into_merge} == {"spawned", "inferred_fanin"}
    assert {e["from"] for e in into_merge} == chunk_keys
    assert sum(e["kind"] == "inferred_fanin" for e in into_merge) == 2


# ---- retry / reject -------------------------------------------------------------------------------
def test_transient_chunk_shows_two_retries_then_success(celery5):
    run = pipeline_runs(celery5)[1]
    retried = next(t for t in run["tasks"] if t["retries"] == 2)
    assert retried["state"] == "SUCCESS"
    reasons = [s for s in retried["timeline"] if s["state"] == "RETRY" or (s["note"] or "").startswith("retry reason")]
    assert len(reasons) == 2
    moves = [s["state"] for s in retried["timeline"] if s["transition"]]
    assert moves.count("QUEUED") == 3  # first publish + 2 retry publishes


def test_retry_waits_are_visible_as_waiting_for_eta(celery5):
    run = pipeline_runs(celery5)[1]
    retried = next(t for t in run["tasks"] if t["retries"] == 2)
    received = [s for s in retried["timeline"] if s["state"] == "RECEIVED" and s["transition"]]
    assert len(received) == 3  # once per attempt
    assert retried["eta"] is not None


def test_permanent_failure_is_rejected_and_run_failed(celery5):
    run = pipeline_runs(celery5)[2]
    rejected = [t for t in run["tasks"] if t["state"] == "REJECTED"]
    assert len(rejected) == 1
    assert run["status"] == "FAILED" and run["has_failures"]


# ---- worker death ------------------------------------------------------------------------------------
def test_redelivery_after_kill_is_detected(celery5):
    run = pipeline_runs(celery5)[3]
    redelivered = [t for t in run["tasks"] if t["redelivered"]]
    assert redelivered, "the chunk running on the SIGKILLed worker must show as redelivered"
    task = redelivered[0]
    workers = [s["worker"] for s in task["timeline"] if s["state"] in ("RECEIVED", "STARTED") and s["worker"]]
    assert len(set(workers)) >= 2, "a different worker finished it"
    assert task["state"] == "SUCCESS"


def test_task_on_a_silent_worker_becomes_worker_lost(celery5):
    # Replay only up to the moment before the redelivery, then look at the victim's task.
    events = events_of("celery5_pipeline")
    full = build_store("celery5_pipeline")
    victim = next(t for t in full.tasks.values() if t.redelivered)
    first_worker = next(s.worker for s in victim.timeline if s.state == "STARTED")
    # find the first event of a task-received for this uuid whose host differs from the first worker
    cut = next(i for i, e in enumerate(events) if e["type"] == "task-received" and e["uuid"] == victim.uuid and e["hostname"] != first_worker and i > 0)
    store = replay_into(RunStore(), events[:cut])
    task = store.tasks[victim.key]
    assert task.state == "STARTED" and task.worker == first_worker
    worker = store.workers[first_worker]
    # one minute later with no heartbeat: lost. Moments after the last heartbeat: not lost.
    assert effective_state(task, store, worker.last_seen_local + 60)[0] == "WORKER_LOST"
    assert effective_state(task, store, worker.last_seen_local + 1)[0] == "STARTED"


def test_workers_report_their_own_heartbeat_period(celery5):
    # The docs say "every minute"; these workers send every 2 s. The threshold must come from the event.
    ws = {w["hostname"].split("@")[0]: w for w in workers_view(celery5)}
    assert "core" in ws and "spot" in ws
    assert all(w["freq"] and 0 < w["freq"] < 60 for w in ws.values())


# ---- robustness -----------------------------------------------------------------------------------------
def test_final_states_do_not_depend_on_arrival_order(celery5):
    ordered = sorted(events_of("celery5_pipeline"), key=lambda e: (e["timestamp"], e.get("clock", 0)))
    other = replay_into(RunStore(), ordered)
    final = lambda s: {k: t.state for k, t in s.tasks.items()}
    assert final(other) == final(celery5)


def test_a_monitor_joining_mid_flight_gets_placeholders_not_crashes():
    store = RunStore()
    store.apply({"type": "task-started", "uuid": "ghost", "hostname": "w@1", "timestamp": 100.0, "local_received": 100.0})
    store.apply({"type": "task-succeeded", "uuid": "ghost", "hostname": "w@1", "runtime": 1.0, "timestamp": 101.0, "local_received": 101.0})
    task = store.tasks["ghost:0"]
    assert task.name is None and task.state == "SUCCESS"


def test_duplicate_delivery_after_success_is_noted_not_regressed():
    store = RunStore()
    for etype, extra in (("task-sent", {"name": "t.x", "root_id": "r", "retries": 0}), ("task-received", {"name": "t.x", "root_id": "r", "retries": 0}),
                         ("task-started", {}), ("task-succeeded", {"runtime": 1.0}),
                         ("task-received", {"name": "t.x", "root_id": "r", "retries": 0}), ("task-started", {})):
        store.apply({"type": etype, "uuid": "u1", "hostname": "w@1", "timestamp": 1.0, "local_received": 1.0, **extra})
    task = store.tasks["u1:0"]
    assert task.state == "SUCCESS" and task.duplicates == 2


def test_unknown_event_types_are_ignored():
    store = RunStore()
    store.apply({"type": "task-from-the-future", "uuid": "u", "timestamp": 1.0})
    store.apply({"type": "something-else"})
    assert not store.tasks


def test_store_is_bounded():
    from celery_viz.config import VizConfig

    store = RunStore(VizConfig(max_runs=3))
    for i in range(10):
        store.apply({"type": "task-sent", "uuid": f"u{i}", "name": "t.x", "root_id": f"u{i}", "retries": 0, "timestamp": float(i), "local_received": float(i)})
    assert len(store.runs) == 3 and len(store.tasks) == 3 and set(store.by_uuid) == {"u7", "u8", "u9"}


def test_hung_detection_is_off_unless_configured(celery5):
    from celery_viz.config import VizConfig

    store = RunStore(VizConfig(hung_after_seconds=10))
    store.apply({"type": "task-sent", "uuid": "h", "name": "t.slow", "root_id": "h", "retries": 0, "timestamp": 0.0, "local_received": 0.0})
    store.apply({"type": "task-received", "uuid": "h", "name": "t.slow", "root_id": "h", "retries": 0, "hostname": "w@1", "timestamp": 0.0, "local_received": 0.0})
    store.apply({"type": "task-started", "uuid": "h", "hostname": "w@1", "timestamp": 1.0, "local_received": 1.0})
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 5, "timestamp": 30.0, "local_received": 30.0})
    task = store.tasks["h:0"]
    assert effective_state(task, store, 5.0)[0] == "STARTED"
    assert effective_state(task, store, 30.0)[0] == "SUSPECTED_HUNG"


# ---- Celery 4.4.7 fleet, 5.6.3 monitor ---------------------------------------------------------------------
def test_celery_447_events_are_understood(celery447):
    assert any("4.4.7" in (w["software"] or "") for w in workers_view(celery447))
    assert all(t.state == "SUCCESS" for t in celery447.tasks.values())


def test_447_chain_is_linked_but_client_built_chord_headers_are_not_grouped(celery447):
    runs = [run_view(celery447, r) for r in runs_by_start(celery447)]
    sizes = sorted(len(r["tasks"]) for r in runs)
    # chain: 2 tasks in ONE run. chord: three header tasks that are each their OWN root (Celery 4.4.7 gives
    # client-submitted chord headers no shared root_id), plus the callback attached to the last one.
    assert sizes == [1, 1, 2, 2]
    chain = next(r for r in runs if len(r["tasks"]) == 2 and not any(t["name"].endswith("tsum") for t in r["tasks"]))
    assert chain["edges"] and chain["edges"][0]["kind"] == "spawned"


def test_crash_looping_worker_flaps_online_offline_without_breaking_the_store():
    store = build_store("celery447_on_rabbitmq43_flapping")
    assert len(store.workers) >= 1
    assert not store.tasks


def test_a_runaway_run_is_capped_without_killing_ingestion():
    from celery_viz.config import VizConfig

    store = RunStore(VizConfig(max_tasks_per_run=3))
    for i in range(10):
        store.apply({"type": "task-sent", "uuid": f"c{i}", "name": "t.child", "root_id": "big", "parent_id": "big", "retries": 0, "timestamp": float(i), "local_received": float(i)})
    store.apply({"type": "task-sent", "uuid": "other", "name": "t.x", "root_id": "other", "retries": 0, "timestamp": 20.0, "local_received": 20.0})
    assert len(store.runs["big"].task_keys) == 3
    assert store.dropped == 7
    assert "other:0" in store.tasks, "later events for other runs are still processed"


def test_eviction_also_cleans_the_started_index():
    from celery_viz.config import VizConfig

    store = RunStore(VizConfig(max_runs=1))
    for uid in ("a", "b"):
        for etype, extra in (("task-sent", {"name": "t.x", "root_id": uid, "retries": 0}), ("task-received", {"name": "t.x", "root_id": uid, "retries": 0}), ("task-started", {})):
            store.apply({"type": etype, "uuid": uid, "hostname": "w@1", "timestamp": 1.0, "local_received": 1.0, **extra})
    assert "a:0" not in store.tasks
    assert store.started_on == {"w@1": {"b:0"}}


def _task_running_on(store, host="w@1", started=10.0):
    for etype, extra in (("task-sent", {"name": "t.x", "root_id": "u", "retries": 0}), ("task-received", {"name": "t.x", "root_id": "u", "retries": 0}), ("task-started", {})):
        store.apply({"type": etype, "uuid": "u", "hostname": host, "timestamp": started, "local_received": started, **extra})
    return store.tasks["u:0"]


def test_a_short_heartbeat_without_active_cannot_make_a_running_task_look_finished():
    # Regression found by live validation: workers interleave full heartbeats (with `active`) and short
    # ones (without). A stale active=0 plus a fresh short heartbeat used to flag a RUNNING task.
    from celery_viz.derive import silent_ended_keys

    store = RunStore()
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 2, "active": 0, "processed": 0, "timestamp": 9.0, "local_received": 9.0})
    task = _task_running_on(store, started=10.0)
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 2, "timestamp": 14.0, "local_received": 14.0})  # NO active field
    assert silent_ended_keys(store) == set(), "the only active=0 report predates the task"
    assert effective_state(task, store, 14.0)[0] == "STARTED"
    # A heartbeat that DOES report active=0 after the start is real evidence.
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 2, "active": 0, "processed": 0, "timestamp": 16.0, "local_received": 16.0})
    assert silent_ended_keys(store) == {task.key}


def test_a_running_task_is_never_flagged_silent_while_replaying_real_heartbeats(celery5):
    # Same check as the live validation: flag-as-you-go over the whole recording; no chunk may ever be flagged.
    from celery_viz.derive import silent_ended_keys

    store = RunStore()
    for event in events_of("celery5_pipeline"):
        store.apply(event)
        for key in silent_ended_keys(store):
            assert store.tasks[key].name != "tasks.analyze_chunk", key


def test_warm_shutdown_is_draining_not_lost_and_a_missed_grace_period_is_lost():
    # Regression found by live validation: worker-offline arrives at the START of a warm shutdown, while the
    # in-flight task is still running (and later succeeds). Calling that "lost" would flag every deploy.
    store = RunStore()
    task = _task_running_on(store, host="w@1", started=10.0)
    store.apply({"type": "worker-heartbeat", "hostname": "w@1", "freq": 2, "active": 1, "processed": 0, "timestamp": 11.0, "local_received": 11.0})
    store.apply({"type": "worker-offline", "hostname": "w@1", "freq": 2, "active": 1, "timestamp": 12.0, "local_received": 12.0})
    assert effective_state(task, store, 15.0)[0] == "DRAINING"
    assert effective_state(task, store, 12.0 + store.config.drain_grace_seconds + 1)[0] == "WORKER_LOST", "grace period ran out: it was killed"
    store.apply({"type": "task-succeeded", "uuid": "u", "hostname": "w@1", "runtime": 5.0, "timestamp": 16.0, "local_received": 16.0})
    assert effective_state(task, store, 17.0)[0] == "SUCCESS"


def test_a_worker_that_comes_back_is_no_longer_draining():
    store = RunStore()
    task = _task_running_on(store, host="w@1", started=10.0)
    store.apply({"type": "worker-offline", "hostname": "w@1", "freq": 2, "timestamp": 12.0, "local_received": 12.0})
    store.apply({"type": "worker-online", "hostname": "w@1", "freq": 2, "timestamp": 13.0, "local_received": 13.0})
    assert store.workers["w@1"].offline_since_local is None
    assert effective_state(task, store, 14.0)[0] == "STARTED"


def test_ignored_task_names_vanish_including_their_later_nameless_events():
    from celery_viz.config import VizConfig

    store = RunStore(VizConfig(ignore_tasks=("t.noise",)))
    for etype, extra in (("task-sent", {"name": "t.noise", "root_id": "n"}), ("task-received", {"name": "t.noise", "root_id": "n"}), ("task-started", {}), ("task-succeeded", {"runtime": 0.1})):
        store.apply({"type": etype, "uuid": "n", "hostname": "w@1", "retries": 0, "timestamp": 1.0, "local_received": 1.0, **extra})
    store.apply({"type": "task-sent", "uuid": "real", "name": "t.real", "root_id": "real", "retries": 0, "timestamp": 2.0, "local_received": 2.0})
    assert set(store.tasks) == {"real:0"}, "started/succeeded carry no name, so the remembered id must be what filters them"


def test_a_reserved_task_on_a_shutting_down_worker_goes_back_to_the_queue_not_draining():
    # Found live: only RUNNING tasks drain in a warm shutdown; reserved ones are requeued by the broker.
    store = RunStore()
    for etype, extra in (("task-sent", {"name": "t.x", "root_id": "u", "retries": 0}), ("task-received", {"name": "t.x", "root_id": "u", "retries": 0})):
        store.apply({"type": etype, "uuid": "u", "hostname": "w@1", "timestamp": 10.0, "local_received": 10.0, **extra})
    store.apply({"type": "worker-offline", "hostname": "w@1", "freq": 2, "timestamp": 12.0, "local_received": 12.0})
    state, note = effective_state(store.tasks["u:0"], store, 14.0)
    assert state == "QUEUED" and "go back on the queue" in note


def test_a_huge_fan_out_renders_in_linear_time():
    # Regression found by load testing: fan-in inference scanned every task for every task, so a run with 2000
    # parallel tasks took ~470 ms per view, recomputed every second while holding the store lock.
    import time

    store = RunStore()
    clock = [1000.0]

    def ev(etype, uuid, **kw):
        clock[0] += 0.001
        store.apply({"type": etype, "uuid": uuid, "hostname": "w@1", "retries": 0, "timestamp": clock[0], "local_received": clock[0], **kw})

    ev("task-sent", "root", name="tasks.parent", root_id="root")
    n = 1500
    for i in range(n):
        ev("task-sent", f"c{i}", name="tasks.child", root_id="root", parent_id="root")
        ev("task-received", f"c{i}", name="tasks.child", root_id="root", parent_id="root")
        ev("task-started", f"c{i}")
        ev("task-succeeded", f"c{i}", runtime=1.0)
    ev("task-sent", "root", name="tasks.join", root_id="root", parent_id=f"c{n - 1}")
    start = time.perf_counter()
    view = run_view(store, store.runs["root"])
    elapsed = time.perf_counter() - start
    joins = [e for e in view["edges"] if e["kind"] == "inferred_fanin"]
    assert len(joins) == n - 1, "every other finished sibling is joined to the callback"
    assert elapsed < 0.25, f"run_view took {elapsed * 1000:.0f} ms for {n} tasks (it was ~250 ms+ when quadratic)"
