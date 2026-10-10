"""Queue depth from RabbitMQ's own Prometheus plugin: reused, not rebuilt.

Events cannot say when a message is acknowledged. The broker can: `unacked` is "delivered to a consumer
but not yet acknowledged". That is the *measured* half of the acks_late story, so we read it from the
`rabbitmq_prometheus` endpoint (enabled in the official RabbitMQ image) rather than counting ourselves.
"""
from __future__ import annotations

import re
import urllib.request
from typing import Any

from prometheus_client.parser import text_string_to_metric_families

READY = "rabbitmq_detailed_queue_messages_ready"
UNACKED = "rabbitmq_detailed_queue_messages_unacked"
# Celery's own housekeeping queues are noise for a workflow view.
_INTERNAL = re.compile(r"(^celeryev\.)|(\.pidbox$)|(^celery_delayed_)")


def parse_queue_metrics(body: str) -> list[dict[str, Any]]:
    ready: dict[str, float] = {}
    unacked: dict[str, float] = {}
    for family in text_string_to_metric_families(body):
        for sample in family.samples:
            queue = sample.labels.get("queue")
            if not queue or _INTERNAL.search(queue):
                continue
            if sample.name == READY:
                ready[queue] = sample.value
            elif sample.name == UNACKED:
                unacked[queue] = sample.value
    return [{"queue": q, "ready": int(ready.get(q, 0)), "unacked": int(unacked.get(q, 0))} for q in sorted(set(ready) | set(unacked))]


def fetch_queue_stats(url: str, timeout: float = 2.0) -> list[dict[str, Any]]:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310 (operator-supplied URL)
        return parse_queue_metrics(resp.read().decode())
