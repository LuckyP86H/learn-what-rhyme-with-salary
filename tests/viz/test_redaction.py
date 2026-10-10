from celery_viz.redaction import RedactionPolicy, redact

EVENT = {"type": "task-succeeded", "uuid": "u", "args": ["transcript with PII"], "kwargs": {"email": "a@b.c"},
         "result": {"summary": "secret"}, "traceback": "File x.py line 1", "exception": "ValueError('boom')", "runtime": 1.5}


def test_payloads_are_dropped_by_default():
    out = redact(EVENT)
    for key in ("args", "kwargs", "result", "traceback"):
        assert key not in out
    assert out["exception"] == "ValueError('boom')"
    assert out["runtime"] == 1.5 and out["uuid"] == "u"


def test_does_not_mutate_the_input():
    redact(EVENT)
    assert "args" in EVENT


def test_opt_in_keeps_payloads_but_never_tracebacks():
    out = redact(EVENT, RedactionPolicy(keep_args=True, keep_result=True))
    assert out["args"] and out["kwargs"] and out["result"]
    assert "traceback" not in out


def test_exception_text_is_truncated_or_removed():
    long = dict(EVENT, exception="x" * 1000)
    assert len(redact(long)["exception"]) == 200
    assert "exception" not in redact(long, RedactionPolicy(keep_exception=False))
