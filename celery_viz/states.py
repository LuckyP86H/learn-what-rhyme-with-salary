"""Loads and validates states.json: the declarative description of every task state."""
from __future__ import annotations

import json
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Any

_PATH = Path(__file__).with_name("states.json")
_REQUIRED = ("key", "label", "kind", "celery", "station", "phase", "tone", "summary", "detail", "settings")
FALLBACK = "UNKNOWN"


@dataclass(frozen=True)
class StateInfo:
    key: str
    label: str
    kind: str  # "celery" (Celery reports it) | "derived" (this monitor infers it) | "fallback"
    celery: dict[str, Any]
    station: str
    phase: str
    tone: str
    summary: str
    detail: str
    settings: list[dict[str, str]]


@dataclass(frozen=True)
class StateTable:
    docs: dict[str, str]
    stations: list[dict[str, str]]
    phases: dict[str, str]
    states: dict[str, StateInfo]

    def get(self, key: str) -> StateInfo:
        """Unknown keys never raise: they render as the generic fallback state."""
        return self.states.get(key) or self.states[FALLBACK]

    def to_json(self) -> dict[str, Any]:
        return {
            "docs": self.docs,
            "stations": self.stations,
            "phases": self.phases,
            "states": [s.__dict__ for s in self.states.values()],
        }


def _validate(raw: dict[str, Any]) -> StateTable:
    stations = {s["key"] for s in raw["stations"]}
    states: dict[str, StateInfo] = {}
    for item in raw["states"]:
        missing = [k for k in _REQUIRED if k not in item]
        if missing:
            raise ValueError(f"state {item.get('key')!r} is missing {missing}")
        if item["key"] in states:
            raise ValueError(f"duplicate state {item['key']!r}")
        if item["station"] not in stations:
            raise ValueError(f"state {item['key']!r}: unknown station {item['station']!r}")
        if item["phase"] not in raw["phases"]:
            raise ValueError(f"state {item['key']!r}: unknown phase {item['phase']!r}")
        states[item["key"]] = StateInfo(**{k: item[k] for k in _REQUIRED})
    if FALLBACK not in states:
        raise ValueError(f"states.json must define the {FALLBACK} fallback")
    return StateTable(raw["docs"], raw["stations"], raw["phases"], states)


@cache
def load_states() -> StateTable:
    return _validate(json.loads(_PATH.read_text()))
