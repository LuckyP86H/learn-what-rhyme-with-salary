from pathlib import Path

import pytest

from celery_viz.config import VizConfig
from celery_viz.replay import load_events, replay_into
from celery_viz.store import RunStore

FIXTURES = Path(__file__).parent / "fixtures"


def events_of(name):
    return list(load_events(FIXTURES / f"{name}.jsonl"))


def build_store(name, config=None):
    return replay_into(RunStore(config or VizConfig()), events_of(name))


@pytest.fixture(scope="module")
def celery5():
    return build_store("celery5_pipeline")


@pytest.fixture(scope="module")
def celery447():
    return build_store("celery447_chord_chain")
