"""EventCollector: listens to a live broker's Celery event stream and feeds a RunStore.

Runs as a daemon thread with its own Celery app (the monitor never imports the monitored project's
code), reconnecting with backoff, because a monitor that dies with the broker connection is useless
exactly when you need it.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

from celery import Celery

from celery_viz.redaction import RedactionPolicy, redact
from celery_viz.store import RunStore

log = logging.getLogger(__name__)


class EventCollector(threading.Thread):
    def __init__(self, broker_url: str, store: RunStore, policy: RedactionPolicy | None = None):
        super().__init__(name="celery-viz-collector", daemon=True)
        self.store = store
        self.policy = policy or RedactionPolicy()
        self.stop_event = threading.Event()
        self.connected = False
        self.events_seen = 0
        self.connect_attempts = 0
        self.last_error: str | None = None
        self.receiver: Any = None
        self.app = Celery("celery_viz_monitor", broker=broker_url)
        # RabbitMQ 4.3+ refuses non-durable, non-exclusive queues, which is what Celery's event queue is
        # by default. Each receiver's queue is only ever used by that receiver, so exclusive is correct.
        self.app.conf.event_queue_exclusive = True
        self.app.conf.broker_connection_retry_on_startup = True

    def _handle(self, event: dict[str, Any]) -> None:
        self.events_seen += 1
        try:
            self.store.apply(redact(event, self.policy))
        except Exception:  # one malformed event must never end the stream
            log.exception("could not apply event type=%s", event.get("type"))

    def run(self) -> None:
        delay = 1.0
        while not self.stop_event.is_set():
            self.connect_attempts += 1
            try:
                with self.app.connection_for_read() as conn:
                    self.receiver = self.app.events.Receiver(conn, handlers={"*": self._handle})
                    self.connected, self.last_error, delay = True, None, 1.0
                    log.info("collecting events from %s", conn.as_uri())
                    # timeout=None: kombu then handles idle seconds itself and keeps ONE consumer open until
                    # should_stop is set. Re-entering capture() on every idle timeout (as an earlier version
                    # did) cancels the consumer, which auto-deletes the exclusive event queue; events
                    # published in that gap have no queue to land in and are silently lost.
                    self.receiver.capture(limit=None, timeout=None, wakeup=True)
            except Exception as exc:
                self.connected = False
                self.last_error = f"{type(exc).__name__}: {exc}"[:300]
                log.warning("event stream lost (%s); retrying in %.0fs", self.last_error, delay)
                self.stop_event.wait(delay)
                delay = min(delay * 2, 30.0)
            finally:
                self.receiver = None
        self.connected = False

    def stop(self) -> None:
        self.stop_event.set()
        if self.receiver is not None:
            self.receiver.should_stop = True  # checked about once a second by kombu's consume loop

    def status(self) -> dict[str, Any]:
        return {"connected": self.connected, "events_seen": self.events_seen, "connect_attempts": self.connect_attempts,
                "last_error": self.last_error}


class ReplayDriver(threading.Thread):
    """Feeds a recording into the store at `speed`x, compressing long idle gaps. For demos and tests."""

    def __init__(self, events: list[dict[str, Any]], store: RunStore, policy: RedactionPolicy | None = None,
                 speed: float = 1.0, max_gap: float = 1.5):
        super().__init__(name="celery-viz-replay", daemon=True)
        self.events, self.store, self.policy = events, store, policy or RedactionPolicy()
        self.speed, self.max_gap = max(speed, 0.01), max_gap
        self.stop_event = threading.Event()
        self.events_seen = 0
        self.finished = False

    def run(self) -> None:
        previous: float | None = None
        for event in self.events:
            if self.stop_event.is_set():
                return
            clock = float(event.get("local_received") or event.get("timestamp") or 0.0)
            if previous is not None and clock > previous:
                time.sleep(min((clock - previous) / self.speed, self.max_gap))
            previous = clock
            self.store.apply(redact(event, self.policy))
            self.events_seen += 1
        self.finished = True

    def stop(self) -> None:
        self.stop_event.set()

    def status(self) -> dict[str, Any]:
        return {"connected": not self.finished, "events_seen": self.events_seen, "replay_finished": self.finished,
                "total_events": len(self.events)}
