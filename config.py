"""Configuration for the call-processing pipeline.

Everything tunable comes from environment variables, so the *same image* can be flipped between
"hardened" Celery and Celery's stock defaults without editing code. That flip is the whole point of
the experiments in the README (`make test-spot` vs `make test-spot-default`).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from kombu import Exchange, Queue


def _env(name: str, default: str) -> str:
    # docker-compose renders an unset "${VAR:-}" as an empty string; treat that as "not set".
    value = os.environ.get(name)
    return value if value else default


def _env_opt(name: str) -> str | None:
    return os.environ.get(name) or None


def _queue_type() -> str:
    value = _env("QUEUE_TYPE", "classic").lower()
    if value not in {"classic", "quorum"}:
        raise ValueError(f"QUEUE_TYPE must be 'classic' or 'quorum', got {value!r}")
    return value


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # --- infrastructure -----------------------------------------------------------------------
    database_url: str
    broker_url: str
    result_backend: str
    rabbitmq_api_url: str
    rabbitmq_user: str
    rabbitmq_password: str
    log_level: str
    queue_type: str  # "classic" | "quorum"

    # --- reliability profile (see RELIABILITY_PROFILES) ----------------------------------------
    reliability_mode: str  # "hardened" | "default"
    acks_late_override: bool | None
    reject_on_worker_lost_override: bool | None
    prefetch_multiplier_override: int | None

    # --- pipeline behaviour ---------------------------------------------------------------------
    chunk_min_seconds: float  # simulated "transcription + diarization" time per chunk
    chunk_max_seconds: float
    slow_chunk_seconds: float  # duration of a chunk carrying the "slow" fault
    heartbeat_interval: float  # how often a running chunk renews its lease
    lease_seconds: float  # a PROCESSING chunk with a heartbeat older than this is considered orphaned
    lease_retry_seconds: float  # how long a duplicate delivery waits before re-checking a busy chunk
    max_attempts: int  # per-chunk executions (incl. crashes/redeliveries) before it goes to the DLQ
    retry_backoff_base: float
    retry_backoff_max: float

    # --- task police ------------------------------------------------------------------------------
    police_interval_seconds: float
    stall_threshold_seconds: float
    max_recoveries: int

    @classmethod
    def from_env(cls) -> Settings:
        def opt_bool(name: str) -> bool | None:
            raw = _env_opt(name)
            return None if raw is None else _truthy(raw)

        raw_prefetch = _env_opt("CELERY_PREFETCH_MULTIPLIER")
        mode = _env("RELIABILITY_MODE", "hardened").lower()
        if mode not in RELIABILITY_PROFILES:
            raise ValueError(f"RELIABILITY_MODE must be one of {sorted(RELIABILITY_PROFILES)}, got {mode!r}")
        return cls(
            database_url=_env("DATABASE_URL", "postgresql+psycopg://pipeline:pipeline@localhost:5432/pipeline"),
            broker_url=_env("BROKER_URL", "amqp://pipeline:pipeline@localhost:5672//"),
            result_backend=_env("RESULT_BACKEND", "redis://localhost:6379/0"),
            rabbitmq_api_url=_env("RABBITMQ_API_URL", "http://localhost:15672/api"),
            rabbitmq_user=_env("RABBITMQ_USER", "pipeline"),
            rabbitmq_password=_env("RABBITMQ_PASSWORD", "pipeline"),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
            queue_type=_queue_type(),
            reliability_mode=mode,
            acks_late_override=opt_bool("CELERY_ACKS_LATE"),
            reject_on_worker_lost_override=opt_bool("CELERY_REJECT_ON_WORKER_LOST"),
            prefetch_multiplier_override=int(raw_prefetch) if raw_prefetch else None,
            chunk_min_seconds=float(_env("CHUNK_MIN_SECONDS", "5")),
            chunk_max_seconds=float(_env("CHUNK_MAX_SECONDS", "10")),
            slow_chunk_seconds=float(_env("SLOW_CHUNK_SECONDS", "22")),
            heartbeat_interval=float(_env("HEARTBEAT_INTERVAL_SECONDS", "1")),
            lease_seconds=float(_env("LEASE_SECONDS", "10")),
            lease_retry_seconds=float(_env("LEASE_RETRY_SECONDS", "3")),
            max_attempts=int(_env("MAX_ATTEMPTS", "4")),
            retry_backoff_base=float(_env("RETRY_BACKOFF_BASE", "2")),
            retry_backoff_max=float(_env("RETRY_BACKOFF_MAX", "60")),
            police_interval_seconds=float(_env("POLICE_INTERVAL_SECONDS", "60")),
            stall_threshold_seconds=float(_env("STALL_THRESHOLD_SECONDS", "120")),
            max_recoveries=int(_env("MAX_RECOVERIES", "3")),
        )


# Two named bundles of the three settings that decide whether work survives a worker dying.
# "default" is *exactly* what Celery ships with; it is here so you can watch it fail.
RELIABILITY_PROFILES: dict[str, dict[str, Any]] = {
    "hardened": {
        "task_acks_late": True,
        "task_reject_on_worker_lost": True,
        "worker_prefetch_multiplier": 1,
        "worker_cancel_long_running_tasks_on_connection_loss": True,
    },
    "default": {
        "task_acks_late": False,
        "task_reject_on_worker_lost": False,
        "worker_prefetch_multiplier": 4,
        "worker_cancel_long_running_tasks_on_connection_loss": False,
    },
}

settings = Settings.from_env()

# --- queues ---------------------------------------------------------------------------------------
# Work is split by *kind*, not by importance, so each kind can run on different hardware:
#   heavy_processing  long CPU/IO chunks      -> the "Spot fleet" (worker-spot), the thing we kill
#   default           ingest + chord callback -> the "on-demand" worker (worker-core)
#   priority          the Task Police sweep   -> same on-demand worker, never stuck behind chunks
#   dead_letter       nobody consumes it; it is where rejected/poisoned messages are parked
# Exchange type is "topic" (not "direct"): exact routing keys behave identically, but Celery's native
# delayed delivery (the quorum-queue way to hold ETA/countdown tasks, i.e. our retry backoff) refuses
# direct exchanges and silently falls back to blocking a worker slot until the ETA arrives.
TASK_EXCHANGE = Exchange("tasks", type="topic")
DEAD_LETTER_EXCHANGE = Exchange("dlx", type="topic")

# Dead-lettering is a *queue argument*: when a consumer rejects a message with requeue=False,
# RabbitMQ re-publishes it to this exchange instead of dropping it. Queue arguments are immutable
# after declaration; changing them (including the queue TYPE) needs a fresh broker, otherwise
# RabbitMQ answers PRECONDITION_FAILED.
_DLX_ARGS = {"x-dead-letter-exchange": "dlx", "x-dead-letter-routing-key": "dead"}

# QUEUE_TYPE=quorum: the replicated, Raft-based queue type RabbitMQ and Celery recommend long term
# (classic queues + global QoS are on the deprecation path). Behavioural differences that matter here:
#   * Celery detects quorum queues and turns global QoS OFF, so prefetch becomes a static
#     per-consumer limit (no autoscaling, no prefetch-count reduction);
#   * ETA/countdown tasks (our retry backoff!) go through Celery's "native delayed delivery";
#   * RabbitMQ enforces a delivery limit (x-delivery-limit, default 20): a broker-side poison-pill guard.
_QUEUE_TYPE_ARGS = {"x-queue-type": "quorum"} if settings.queue_type == "quorum" else {}

TASK_QUEUES = (
    Queue("default", TASK_EXCHANGE, routing_key="default", queue_arguments={**_DLX_ARGS, **_QUEUE_TYPE_ARGS}),
    Queue("heavy_processing", TASK_EXCHANGE, routing_key="heavy_processing", queue_arguments={**_DLX_ARGS, **_QUEUE_TYPE_ARGS}),
    Queue("priority", TASK_EXCHANGE, routing_key="priority", queue_arguments={**_DLX_ARGS, **_QUEUE_TYPE_ARGS}),
    Queue("dead_letter", DEAD_LETTER_EXCHANGE, routing_key="dead", queue_arguments=dict(_QUEUE_TYPE_ARGS)),
)

TASK_ROUTES = {
    "tasks.ingest_call": {"queue": "default"},
    "tasks.analyze_chunk": {"queue": "heavy_processing"},
    "tasks.merge_and_finalize": {"queue": "default"},
    "tasks.on_pipeline_failure": {"queue": "default"},
    "tasks.police_sweep": {"queue": "priority"},
    # plain demo tasks for the visualizer's scenarios (app/tasks/meeting.py)
    "tasks.meeting_summarize_one": {"queue": "default"},
    "tasks.meeting_transcribe": {"queue": "heavy_processing"},
    "tasks.meeting_summarize": {"queue": "default"},
    "tasks.meeting_extract_actions": {"queue": "default"},
    "tasks.meeting_translate": {"queue": "heavy_processing"},
    "tasks.meeting_translate_all": {"queue": "default"},
}


def build_celery_config(s: Settings = settings) -> dict[str, Any]:
    profile = dict(RELIABILITY_PROFILES[s.reliability_mode])
    if s.acks_late_override is not None:
        profile["task_acks_late"] = s.acks_late_override
    if s.reject_on_worker_lost_override is not None:
        profile["task_reject_on_worker_lost"] = s.reject_on_worker_lost_override
    if s.prefetch_multiplier_override is not None:
        profile["worker_prefetch_multiplier"] = s.prefetch_multiplier_override

    return {
        "broker_url": s.broker_url,
        "result_backend": s.result_backend,
        # Celery 5.x warns (and 6.x will refuse) if you rely on implicit "retry the broker at startup".
        "broker_connection_retry_on_startup": True,
        # Quorum queues: wait for the broker to confirm each publish, so "published" means "durably accepted".
        "broker_transport_options": {"confirm_publish": True} if s.queue_type == "quorum" else {},
        # Plain JSON everywhere: no pickle, so a poisoned message can't execute code on a worker.
        "task_serializer": "json",
        "result_serializer": "json",
        "accept_content": ["json"],
        "timezone": "UTC",
        "enable_utc": True,

        # ---- the reliability trio (values come from RELIABILITY_PROFILES) ------------------------
        #
        # task_acks_late
        #   Default False: the broker message is ACKed the instant a worker *starts* the task. If the
        #   worker dies mid-task, RabbitMQ believes the work is done -> the task silently vanishes and
        #   the chord waiting on it never completes. True: ACK only after the task returns; if the
        #   consumer's connection drops first, RabbitMQ puts the message back on the queue.
        #   Price: a task can now run more than once, so every task must be idempotent.
        #
        # task_reject_on_worker_lost
        #   Different failure from the one above. Here the worker's *pool child* dies (OOM-killer,
        #   segfault, kill -9) but the parent keeps its broker connection, so RabbitMQ sees nothing.
        #   Default False: Celery records WorkerLostError and ACKs -> task lost. True: Celery rejects
        #   the message *with requeue*. Only meaningful together with acks_late. Danger: a task that
        #   always crashes its child is requeued forever (a "poison pill"); we cap that with the
        #   `attempts` counter in PostgreSQL (see repository.claim_chunk).
        #
        # worker_prefetch_multiplier
        #   Each worker reserves (multiplier x concurrency) messages in advance. Default 4: with 5-10s
        #   chunks, one busy worker hoards messages while another worker sits idle. 1 = fair dispatch.
        #   Note: on RabbitMQ the reserved-but-unstarted messages are NOT lost if the worker dies
        #   (they are unacked, so the broker requeues them); the cost is unfairness, not data loss.
        #
        # worker_cancel_long_running_tasks_on_connection_loss
        #   If the broker connection drops, RabbitMQ requeues our unacked messages and another worker
        #   may pick them up while *we* are still running them. Cancelling the zombie avoids the
        #   duplicate. (Default False in 5.x; Celery 6 flips it to True.)
        **profile,

        # ---- time limits ----------------------------------------------------------------------------
        # Soft limit: raises SoftTimeLimitExceeded inside the task so it can clean up and retry.
        # Hard limit: SIGKILLs the pool child. A hard-killed task is ACKed (acks_on_failure_or_timeout
        # defaults to True), i.e. NOT redelivered; so the soft limit must always fire first.
        # Both only work with the prefork pool, not solo/threads/gevent.
        "task_soft_time_limit": 30,
        "task_time_limit": 60,

        # How long results (and chord bookkeeping!) live in Redis. Must exceed your longest workflow:
        # if the chord's counter expires before the last header task finishes, the callback never fires.
        "result_expires": 3600,
        # Report STARTED (otherwise "PENDING" means both "queued" and "never heard of it").
        "task_track_started": True,
        # Events feed Flower; without these the Flower dashboard is empty.
        "worker_send_task_events": True,
        "task_send_sent_event": True,

        # ---- queues and routing -----------------------------------------------------------------------
        "task_queues": TASK_QUEUES,
        "task_default_queue": "default",
        "task_default_exchange": "tasks",
        "task_default_exchange_type": "topic",
        "task_default_routing_key": "default",
        "task_routes": TASK_ROUTES,

        # ---- RabbitMQ 4.3+ compatibility -------------------------------------------------------------
        # Celery's control (pidbox) and event queues default to durable=False AND exclusive=False. That
        # combination is RabbitMQ's deprecated feature `transient_nonexcl_queues`, which 4.3 REFUSES by
        # default. The failure is nasty: the worker process stays "Up" but loops on reconnect at the
        # "mingle" step and never consumes a task. Making these queues exclusive (each is only ever used
        # by one connection anyway) takes them out of the deprecated category.
        "control_queue_exclusive": True,
        "event_queue_exclusive": True,

        # We install our own JSON logging (app/logging_config.py); stop Celery hijacking the root logger.
        "worker_hijack_root_logger": False,

        # ---- Task Police ------------------------------------------------------------------------------
        "beat_schedule": {
            "task-police": {
                "task": "tasks.police_sweep",
                "schedule": s.police_interval_seconds,
                # If beat is ahead of the workers, don't let stale sweeps pile up behind each other.
                "options": {"queue": "priority", "expires": s.police_interval_seconds},
            }
        },
    }
