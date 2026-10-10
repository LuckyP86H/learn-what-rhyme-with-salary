from datetime import datetime, timedelta, timezone

import pytest
from celery import chord

from app.analysis import merge_results, sentiment_label, simulate_chunk
from app.tasks.pipeline import backoff_seconds, build_workflow
from app.tasks.police import is_stalled

NOW = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---- analysis: determinism is what makes re-running a chunk harmless ------------------------------
def test_chunk_simulation_is_deterministic():
    a = simulate_chunk("call-1", 3, 5, 10)
    b = simulate_chunk("call-1", 3, 5, 10)
    assert a == b
    assert 5 <= a.duration_s <= 10
    assert simulate_chunk("call-1", 4, 5, 10) != a


def test_merge_aggregates_and_is_order_independent():
    results = [simulate_chunk("c", i, 5, 10).result for i in range(5)]
    merged = merge_results(results)
    assert merged["chunks"] == 5
    assert merged["audio_seconds"] == 300
    assert merged == merge_results(list(reversed(results)))
    assert 0 < merged["agent_talk_ratio"] < 1
    assert -1 <= merged["sentiment"] <= 1


def test_merge_rejects_empty_input():
    with pytest.raises(ValueError):
        merge_results([])


@pytest.mark.parametrize("score,label", [(0.9, "positive"), (-0.9, "negative"), (0.0, "neutral")])
def test_sentiment_label(score, label):
    assert sentiment_label(score) == label


# ---- backoff -------------------------------------------------------------------------------------
def test_backoff_grows_exponentially_with_bounded_jitter():
    for attempt, base in [(1, 2), (2, 4), (3, 8)]:
        for _ in range(50):
            assert base * 0.75 <= backoff_seconds(attempt) <= base * 1.25


def test_backoff_is_capped():
    assert backoff_seconds(30) <= 60 * 1.25


# ---- task police ---------------------------------------------------------------------------------
def test_call_with_recent_chunk_heartbeat_is_not_stalled():
    heartbeat = NOW - timedelta(seconds=3)
    assert not is_stalled(NOW - timedelta(minutes=10), [heartbeat], NOW, 120)


def test_call_with_no_signs_of_life_is_stalled():
    old = NOW - timedelta(seconds=300)
    assert is_stalled(old, [old, old - timedelta(seconds=5)], NOW, 120)


def test_call_with_no_chunks_yet_is_judged_by_its_own_row():
    assert is_stalled(NOW - timedelta(seconds=200), [], NOW, 120)
    assert not is_stalled(NOW - timedelta(seconds=10), [], NOW, 120)


# ---- workflow shape ------------------------------------------------------------------------------
def test_workflow_is_a_chord_with_one_header_task_per_chunk():
    wf = build_workflow("call-1", ["a", "b", "c"])
    assert isinstance(wf, chord)
    assert [t.args for t in wf.tasks] == [("a",), ("b",), ("c",)]
    assert all(t.task == "tasks.analyze_chunk" for t in wf.tasks)
    assert wf.body.task == "tasks.merge_and_finalize"
    assert wf.body.args == ("call-1",)  # Celery prepends the header results at call time


def test_workflow_with_nothing_left_runs_only_the_callback():
    wf = build_workflow("call-1", [])
    assert wf.task == "tasks.merge_and_finalize"
    assert wf.args == ([], "call-1")


def test_workflow_attaches_an_error_callback():
    wf = build_workflow("call-1", ["a"])
    assert wf.body.options["link_error"][0]["task"] == "tasks.on_pipeline_failure"
