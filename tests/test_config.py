import pytest

from config import (
    RELIABILITY_PROFILES,
    TASK_QUEUES,
    TASK_ROUTES,
    Settings,
    build_celery_config,
)


def conf(monkeypatch, **env):
    for key in ("RELIABILITY_MODE", "CELERY_ACKS_LATE", "CELERY_REJECT_ON_WORKER_LOST", "CELERY_PREFETCH_MULTIPLIER"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return build_celery_config(Settings.from_env())


def test_hardened_is_the_default(monkeypatch):
    c = conf(monkeypatch)
    assert c["task_acks_late"] is True
    assert c["task_reject_on_worker_lost"] is True
    assert c["worker_prefetch_multiplier"] == 1


def test_default_mode_is_celerys_stock_behaviour(monkeypatch):
    c = conf(monkeypatch, RELIABILITY_MODE="default")
    assert c["task_acks_late"] is False
    assert c["task_reject_on_worker_lost"] is False
    assert c["worker_prefetch_multiplier"] == 4


def test_individual_overrides_beat_the_profile(monkeypatch):
    c = conf(monkeypatch, RELIABILITY_MODE="hardened", CELERY_PREFETCH_MULTIPLIER="4", CELERY_ACKS_LATE="false")
    assert c["worker_prefetch_multiplier"] == 4
    assert c["task_acks_late"] is False
    assert c["task_reject_on_worker_lost"] is True  # untouched


def test_empty_env_var_means_unset(monkeypatch):
    # docker-compose renders an unset "${VAR:-}" as "".
    c = conf(monkeypatch, CELERY_PREFETCH_MULTIPLIER="", CELERY_ACKS_LATE="")
    assert c["worker_prefetch_multiplier"] == 1
    assert c["task_acks_late"] is True


def test_unknown_mode_is_rejected(monkeypatch):
    monkeypatch.setenv("RELIABILITY_MODE", "yolo")
    with pytest.raises(ValueError):
        Settings.from_env()


def test_soft_limit_fires_before_hard_limit(monkeypatch):
    c = conf(monkeypatch)
    assert c["task_soft_time_limit"] < c["task_time_limit"]


def test_profiles_differ_only_in_reliability_knobs():
    assert RELIABILITY_PROFILES["hardened"].keys() == RELIABILITY_PROFILES["default"].keys()


def test_work_queues_dead_letter_to_dlx_and_dlq_itself_does_not():
    by_name = {q.name: q for q in TASK_QUEUES}
    for name in ("default", "heavy_processing", "priority"):
        assert by_name[name].queue_arguments["x-dead-letter-exchange"] == "dlx"
    # A DLQ that dead-letters into itself would loop forever.
    assert not by_name["dead_letter"].queue_arguments


def test_every_registered_task_has_a_route():
    from app.celery_app import celery_app

    celery_app.loader.import_default_modules()
    ours = {name for name in celery_app.tasks if name.startswith("tasks.")}
    assert ours == set(TASK_ROUTES)
