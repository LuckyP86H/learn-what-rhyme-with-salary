"""Strip sensitive payloads from events BEFORE they reach the store.

Celery task events carry `args`, `kwargs`, `result` and `traceback`. In a real product those can be
transcripts, emails or customer ids. A monitor that stores them becomes a second copy of your data,
so the default is to drop them; opt in explicitly per deployment.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_PAYLOAD_KEYS = ("args", "kwargs", "result")


@dataclass(frozen=True)
class RedactionPolicy:
    keep_args: bool = False  # keeps args AND kwargs
    keep_result: bool = False
    keep_exception: bool = True  # the exception's one-line text; useful and usually safe
    max_text: int = 200


def redact(event: dict[str, Any], policy: RedactionPolicy = RedactionPolicy()) -> dict[str, Any]:
    out = dict(event)
    if not policy.keep_args:
        out.pop("args", None)
        out.pop("kwargs", None)
    if not policy.keep_result:
        out.pop("result", None)
    out.pop("traceback", None)  # full tracebacks leak code paths and local values
    if "exception" in out:
        if policy.keep_exception and out["exception"]:
            out["exception"] = str(out["exception"])[: policy.max_text]
        else:
            out.pop("exception")
    return out
