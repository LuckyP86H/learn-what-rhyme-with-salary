"""Replay recorded events (one JSON object per line) into a store: demos and tests with no broker."""
from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any

from celery_viz.redaction import RedactionPolicy, redact
from celery_viz.store import RunStore


def load_events(path: str | Path) -> Iterator[dict[str, Any]]:
    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def replay_into(store: RunStore, events: Iterable[dict[str, Any]], policy: RedactionPolicy = RedactionPolicy()) -> RunStore:
    for event in events:
        store.apply(redact(event, policy))
    return store
