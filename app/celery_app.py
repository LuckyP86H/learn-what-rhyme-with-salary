"""The Celery application. All reliability-relevant settings live in config.build_celery_config()."""
from __future__ import annotations

from celery import Celery
from celery.signals import setup_logging, worker_process_init

from app.logging_config import configure_logging
from config import build_celery_config, settings

celery_app = Celery("call_pipeline", include=["app.tasks.pipeline", "app.tasks.police", "app.tasks.meeting"])
celery_app.conf.update(build_celery_config())


@setup_logging.connect
def _json_logging(**_: object) -> None:
    # Connecting to this signal tells Celery "I'll configure logging myself".
    configure_logging(settings.log_level)


@worker_process_init.connect
def _reset_db_pool(**_: object) -> None:
    # The prefork pool forks children from the parent. A DB connection must never be shared across a
    # fork, so every child starts with a fresh pool.
    from database import engine

    engine.dispose(close=False)
