"""The scenarios the visualizer can launch in the demo stack (enable with `--catalog app.viz_catalog:catalog`).

Each scenario is a *named, fixed* workflow shape. The point is to see how Celery's different canvas
primitives look when they run: a single task, a chain, a group, and a chord.
"""
from __future__ import annotations

import uuid
from typing import Any

from celery import chain, group

from app import calls
from app.tasks import meeting

_FAULT = {"name": "fault", "label": "Make one task misbehave", "type": "select", "default": "none",
          "options": list(meeting.FAULTS)}
_CHORD_FAULT = {"name": "fault", "label": "Make one chunk misbehave", "type": "select", "default": "none",
                "options": ["none", "transient", "permanent", "crash", "hang", "slow"]}

SCENARIOS: list[dict[str, Any]] = [
    {"id": "single_task", "title": "Single task",
     "description": "Summarize one meeting. One task, one worker: the simplest possible Celery lifecycle.",
     "params": [_FAULT]},
    {"id": "chain", "title": "Chain (one after another)",
     "description": "Transcribe, then summarize, then extract action items. Each step starts only when the previous one succeeds.",
     "params": [_FAULT]},
    {"id": "group", "title": "Group (several at once)",
     "description": "Translate the summary into three languages in parallel. Independent tasks, no join.",
     "params": [_FAULT]},
    {"id": "chord", "title": "Chord (fan-out, then join)",
     "description": "Analyze N audio chunks in parallel, then merge the results once ALL are done. This is the hardened call pipeline.",
     "params": [{"name": "chunks", "label": "Chunks", "type": "int", "default": 4, "min": 1, "max": 12}, _CHORD_FAULT]},
]


class DemoCatalog:
    def describe(self) -> list[dict[str, Any]]:
        return SCENARIOS

    def launch(self, scenario_id: str, params: dict[str, Any]) -> dict[str, Any]:
        mid = f"mtg-{uuid.uuid4().hex[:6]}"
        fault = params.get("fault")
        fault = None if fault in (None, "none") else fault
        if scenario_id == "single_task":
            result = meeting.summarize_meeting.apply_async((mid, fault))
            return {"root_id": result.id}
        if scenario_id == "chain":
            result = chain(meeting.transcribe.s(mid), meeting.summarize.s(fault), meeting.extract_actions.s()).apply_async()
            return {"root_id": result.parent.parent.id if result.parent and result.parent.parent else result.id}
        if scenario_id == "group":
            sigs = [meeting.translate.s(mid, lang, fault if i == 0 else None) for i, lang in enumerate(("es", "de", "ja"))]
            group(sigs).apply_async()
            return {"meeting": mid}
        if scenario_id == "chord":
            n = int(params["chunks"])
            faults = {} if fault is None else {str(min(1, n - 1)): fault}
            return {"call_id": calls.start_call(f"viz {mid}", n, faults)}
        raise ValueError(f"unknown scenario {scenario_id!r}")


catalog = DemoCatalog()
