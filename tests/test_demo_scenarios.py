"""The visualizer's demo scenarios: plain tasks and the catalog that launches them (no broker needed)."""
import pytest
from celery import Task

from app import viz_catalog
from app.tasks import meeting
from celery_viz.catalog import clean_params


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(meeting.time, "sleep", lambda s: None)


def test_single_task_succeeds():
    out = meeting.summarize_meeting.apply(args=("m1", None)).get()
    assert out["meeting"] == "m1"


def test_transient_fault_is_retried_and_then_succeeds():
    result = meeting.summarize_meeting.apply(args=("m1", "transient"))
    assert result.successful()
    assert result.get()["meeting"] == "m1"


def test_permanent_fault_is_an_ordinary_failure():
    result = meeting.summarize_meeting.apply(args=("m1", "permanent"))
    assert result.failed() and isinstance(result.result, ValueError)


def test_results_carry_no_personal_data():
    out = meeting.transcribe.apply(args=("m1",)).get()
    assert set(out) == {"meeting", "words", "seconds"}


def test_every_scenario_has_a_valid_parameter_schema_and_unique_id():
    ids = [s["id"] for s in viz_catalog.SCENARIOS]
    assert len(ids) == len(set(ids)) and {"single_task", "chain", "group", "chord"} <= set(ids)
    for scenario in viz_catalog.SCENARIOS:
        defaults = clean_params(scenario["params"], {})  # defaults must satisfy their own constraints
        assert set(defaults) == {p["name"] for p in scenario["params"]}


def test_unknown_scenario_is_rejected():
    with pytest.raises(ValueError):
        viz_catalog.catalog.launch("rm_rf", {})


def test_chord_scenario_puts_the_fault_on_a_real_chunk(monkeypatch):
    seen = {}
    monkeypatch.setattr(viz_catalog.calls, "start_call", lambda title, n, faults: seen.update(n=n, faults=faults) or "call-1")
    out = viz_catalog.catalog.launch("chord", {"chunks": 1, "fault": "crash"})
    assert out == {"call_id": "call-1"} and seen == {"n": 1, "faults": {"0": "crash"}}, "with one chunk the fault must go on chunk 0, not a missing chunk 1"


def test_group_is_launched_through_one_parent_task_so_it_has_a_single_root(monkeypatch):
    sent = {}

    class Result:
        id = "root-1"

    monkeypatch.setattr(meeting.translate_all, "apply_async", lambda args: sent.update(args=args) or Result())
    out = viz_catalog.catalog.launch("group", {"fault": "permanent"})
    assert out == {"root_id": "root-1"} and sent["args"][1] == "permanent"


def test_the_parent_replaces_itself_with_one_translation_per_language_and_faults_only_the_first(monkeypatch):
    captured = {}

    def fake_replace(self, sig):
        captured["tasks"] = list(sig.tasks)
        return RuntimeError("replaced")  # raise self.replace(...) raises whatever replace returns

    monkeypatch.setattr(Task, "replace", fake_replace)
    with pytest.raises(RuntimeError, match="replaced"):
        meeting.translate_all.run("m1", "permanent")
    tasks = captured["tasks"]
    assert [s.args[1] for s in tasks] == ["es", "de", "ja"]
    assert [s.args[2] for s in tasks] == ["permanent", None, None]
