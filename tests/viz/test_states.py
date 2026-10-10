import copy
import json

import pytest
from celery import states as celery_states

from celery_viz import states as states_module
from celery_viz.states import FALLBACK, _validate, load_states


def test_every_celery_builtin_state_is_described():
    # Drift guard: if a future Celery adds a state, this fails and tells you to describe it in states.json.
    described = {s.celery["state"] for s in load_states().states.values() if s.celery.get("state")}
    builtin = set(celery_states.ALL_STATES) | set(celery_states.UNREADY_STATES)
    assert builtin <= described, f"undescribed Celery states: {sorted(builtin - described)}"


def test_celerys_terminal_states_are_terminal_here():
    table = load_states()
    for state in celery_states.READY_STATES:
        info = next(s for s in table.states.values() if s.celery.get("state") == state)
        assert info.phase in ("done_ok", "done_bad"), state


def test_retry_is_not_terminal():
    # Celery keeps RETRY in UNREADY_STATES; the table must agree or the ball would stop at "Retrying".
    assert "RETRY" in celery_states.UNREADY_STATES
    assert load_states().get("RETRY").phase == "active"


def test_unknown_states_fall_back_instead_of_raising():
    info = load_states().get("MY_CUSTOM_STATE")
    assert info.key == FALLBACK


def test_derived_states_are_marked_as_derived():
    table = load_states()
    for key in ("WAITING_ETA", "REPLACED", "WORKER_LOST", "SUSPECTED_HUNG"):
        assert table.get(key).kind == "derived", key


def _raw():
    return copy.deepcopy(json.loads(states_module._PATH.read_text()))


def test_validation_rejects_unknown_station():
    raw = _raw()
    raw["states"][0]["station"] = "nowhere"
    with pytest.raises(ValueError, match="unknown station"):
        _validate(raw)


def test_validation_rejects_missing_fields_and_missing_fallback():
    raw = _raw()
    del raw["states"][0]["summary"]
    with pytest.raises(ValueError, match="missing"):
        _validate(raw)
    raw = _raw()
    raw["states"] = [s for s in raw["states"] if s["key"] != FALLBACK]
    with pytest.raises(ValueError, match="fallback"):
        _validate(raw)


def test_every_setting_mentioned_has_a_docs_link():
    for info in load_states().states.values():
        for setting in info.settings:
            assert setting["url"].startswith("https://docs.celeryq.dev/"), (info.key, setting["name"])
