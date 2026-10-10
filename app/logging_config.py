"""Structured JSON logging, one object per line, with Celery task context attached."""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

# Attributes every LogRecord has; anything else was passed via `extra=` and belongs in the JSON.
_STANDARD_ATTRS = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "taskName"}


class CeleryContextFilter(logging.Filter):
    """Attach task_name / task_id to every record emitted while a task is running."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            from celery._state import get_current_task

            task = get_current_task()
        except Exception:  # logging must never raise
            task = None
        if task is not None and task.request:
            record.task_name = task.name
            record.task_id = task.request.id
            record.worker = task.request.hostname
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update({k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS and not k.startswith("_")})
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    handler.addFilter(CeleryContextFilter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
