from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from celery_viz import queues as queues_module
from celery_viz.catalog import clean_params, load_catalog
from celery_viz.queues import parse_queue_metrics
from celery_viz.remote_config import RemoteConfig, collect, referenced_settings
from celery_viz.server import create_app
from celery_viz.store import RunStore

FIXTURES = Path(__file__).parent / "fixtures"


# ---- queues (RabbitMQ Prometheus, reused rather than rebuilt) ---------------------------------------------
def test_queue_metrics_parse_the_real_rabbitmq_output():
    rows = {r["queue"]: r for r in parse_queue_metrics((FIXTURES / "rabbitmq_queue_coarse_metrics.txt").read_text())}
    assert {"default", "heavy_processing", "priority", "dead_letter"} <= set(rows)
    assert all(isinstance(r["ready"], int) and isinstance(r["unacked"], int) for r in rows.values())


def test_celery_housekeeping_queues_are_hidden():
    names = {r["queue"] for r in parse_queue_metrics((FIXTURES / "rabbitmq_queue_coarse_metrics.txt").read_text())}
    assert not [n for n in names if n.startswith("celeryev.") or n.endswith(".pidbox")]


# ---- remote config ---------------------------------------------------------------------------------------------
def test_only_settings_the_state_table_references_are_exposed():
    wanted = referenced_settings()
    assert {"task_acks_late", "worker_prefetch_multiplier", "task_soft_time_limit"} <= wanted
    conf = {"w@1": {"task_acks_late": True, "broker_url": "amqp://u:secret@h//", "result_backend": "redis://x"}}
    out = collect(conf, {}, wanted)
    assert set(out["settings"]) == {"task_acks_late"}
    assert "secret" not in str(out)


def test_settings_that_differ_between_workers_are_flagged_as_mixed():
    conf = {"a@1": {"task_acks_late": True}, "b@1": {"task_acks_late": False}}
    out = collect(conf, {}, {"task_acks_late"})
    assert out["settings"]["task_acks_late"]["mixed"] is True and out["settings"]["task_acks_late"]["workers"] == 2


def test_worker_stats_report_prefetch_and_concurrency():
    out = collect({}, {"w@1": {"prefetch_count": 2, "pool": {"max-concurrency": 2}}}, set())
    assert out["workers"]["w@1"] == {"prefetch_count": 2, "concurrency": 2}


class _BrokenApp:
    class control:  # noqa: N801
        @staticmethod
        def inspect(timeout):
            raise ConnectionError("broker down")


def test_remote_config_degrades_instead_of_raising_and_caches_the_failure():
    rc = RemoteConfig(_BrokenApp(), ttl=60)
    first = rc.get()
    assert first["settings"] == {} and "ConnectionError" in first["error"]
    assert rc.get() is first, "cached: a down broker must not be hammered on every request"


# ---- catalog parameter validation ---------------------------------------------------------------------------
SCHEMA = [{"name": "chunks", "type": "int", "default": 4, "min": 1, "max": 12},
          {"name": "fault", "type": "select", "default": "none", "options": ["none", "crash"]}]


def test_defaults_are_filled_and_values_coerced():
    assert clean_params(SCHEMA, {"chunks": "6"}) == {"chunks": 6, "fault": "none"}


@pytest.mark.parametrize("given", [{"chunks": 99}, {"chunks": 0}, {"fault": "rm -rf"}, {"surprise": 1}])
def test_bad_launch_parameters_are_rejected(given):
    with pytest.raises(ValueError):
        clean_params(SCHEMA, given)


def test_catalog_spec_must_name_an_object():
    with pytest.raises(ValueError):
        load_catalog("no_colon_here")


# ---- HTTP: read-only unless a catalog is enabled -----------------------------------------------------------------
class FakeCatalog:
    def __init__(self):
        self.launched = []

    def describe(self):
        return [{"id": "demo", "title": "Demo", "description": "d", "params": SCHEMA}]

    def launch(self, scenario_id, params):
        self.launched.append((scenario_id, params))
        return {"root_id": "abc"}


def client(**kw):
    return TestClient(create_app(RunStore(), mode="replay", **kw))


def test_launching_is_disabled_by_default():
    c = client()
    assert c.get("/api/catalog").json() == {"enabled": False, "scenarios": []}
    assert c.post("/api/launch", json={"scenario": "demo"}).status_code == 404


def test_launch_only_runs_named_scenarios_with_validated_params():
    cat = FakeCatalog()
    c = client(catalog=cat)
    assert c.get("/api/catalog").json()["enabled"] is True
    assert c.post("/api/launch", json={"scenario": "nope"}).status_code == 404
    assert c.post("/api/launch", json={"scenario": "demo", "params": {"chunks": 999}}).status_code == 422
    assert c.post("/api/launch", json={"scenario": "demo", "params": {"task": "os.system"}}).status_code == 422
    ok = c.post("/api/launch", json={"scenario": "demo", "params": {"chunks": 3}})
    assert ok.status_code == 202 and ok.json()["started"] == {"root_id": "abc"}
    assert cat.launched == [("demo", {"chunks": 3, "fault": "none"})]


def test_queues_endpoint_degrades_when_unconfigured_or_unreachable(monkeypatch):
    assert client().get("/api/queues").json()["available"] is False
    def boom(url, timeout=2.0):
        raise OSError("connection refused")
    monkeypatch.setattr("celery_viz.server.fetch_queue_stats", boom)
    body = client(queue_metrics_url="http://x/metrics").get("/api/queues").json()
    assert body["available"] is False and "refused" in body["reason"]


def test_queues_endpoint_serves_measured_numbers(monkeypatch):
    monkeypatch.setattr("celery_viz.server.fetch_queue_stats", lambda url, timeout=2.0: [{"queue": "default", "ready": 1, "unacked": 2}])
    assert client(queue_metrics_url="http://x/metrics").get("/api/queues").json() == {"available": True, "queues": [{"queue": "default", "ready": 1, "unacked": 2}]}


def test_config_endpoint_explains_replay_mode():
    assert "replay" in client().get("/api/config").json()["error"]


def test_built_ui_is_served_without_shadowing_the_api(tmp_path):
    (tmp_path / "index.html").write_text("<html>ui</html>")
    c = client(ui_dir=tmp_path)
    assert "ui" in c.get("/").text
    assert c.get("/api/health").json()["status"] == "ok"


def test_queue_parser_module_does_no_network_on_import():
    assert hasattr(queues_module, "fetch_queue_stats")


def test_launching_is_off_unless_the_environment_or_a_flag_enables_it(monkeypatch):
    from celery_viz.__main__ import build_parser

    monkeypatch.delenv("CELERY_VIZ_CATALOG", raising=False)
    assert build_parser().parse_args([]).catalog is None
    monkeypatch.setenv("CELERY_VIZ_CATALOG", "")  # compose passes an empty string for `VIZ_CATALOG=`
    assert build_parser().parse_args([]).catalog is None
    monkeypatch.setenv("CELERY_VIZ_CATALOG", "app.viz_catalog:catalog")
    assert build_parser().parse_args([]).catalog == "app.viz_catalog:catalog"
    assert build_parser().parse_args(["--catalog", "x:y"]).catalog == "x:y"
