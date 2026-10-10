# Celery vs Redis vs Kafka vs Temporal, using this pipeline as the yardstick

The workload is the one in this repo: ingest a call → analyze N chunks in parallel (5–10 s each, on
machines that can disappear) → merge once all N are done. The question for each tool is the same:

> When a worker dies halfway through chunk 4, **who remembers that chunk 4 still needs to happen, and
> who makes the merge wait for it?**

| | Redis (hand-rolled) | Celery + RabbitMQ | Kafka | Temporal |
|---|---|---|---|---|
| **What it fundamentally is** | Data structures you build a queue from | A task-execution framework over a message broker | A durable, replayable event log | A durable-execution engine |
| **Where "work still to do" lives** | A list/stream you design | A broker message (+ your DB if you are careful) | A log offset | The workflow's event history, in the server's database |
| **Worker dies mid-task** | You build recovery | Message requeued *only if* `acks_late`; child death needs `reject_on_worker_lost` | Offset not committed → redelivered after a rebalance | Server times the activity out and reschedules it |
| **Fan-out / fan-in** | You build it | `chord` (needs a result backend) | You build it (or Kafka Streams/Flink) | `asyncio.gather` in the workflow |
| **Retries with backoff** | You build it | `retry()` / `autoretry_for` | Retry topics you build | `RetryPolicy` |
| **Dead-letter** | You build it | DLX + `Reject(requeue=False)` | Dead-letter *topic* you build | Failed activity/workflow, visible and resettable in the UI |
| **Extra moving parts** | Redis | Broker + result backend (+ beat, Flower) | Kafka cluster | Temporal server + its database |

---

## 1. Celery with RabbitMQ

**Strengths.** The Python-native default: decorate a function, call `.delay()`. Mature routing (queues,
exchanges), canvas primitives (`chain`, `group`, `chord`), periodic tasks (`beat`), rate limits, time
limits, a monitoring UI (Flower), and a huge community. RabbitMQ gives per-message acknowledgement,
dead-lettering, and unambiguous redelivery semantics. For a stateless "do this once, soon" job it is hard to beat.

**The footguns this project made you hit** (each one has a line of code or a comment in the repo):

| Footgun | What actually happens | Where it is handled |
|---|---|---|
| `acks_late=False` (default) | Message ACKed when the task *starts*. Kill the worker → the task is gone, the chord never completes. | `config.py`, experiment 1 |
| `reject_on_worker_lost=False` (default) | A pool *child* dies (OOM) but the parent keeps its connection, so RabbitMQ sees nothing. Celery logs `WorkerLostError` and ACKs. Lost. | `config.py`, experiment 2 |
| `acks_late=True` ⇒ at-least-once | Every task may now run twice, even concurrently. | Lease + fencing token in `repository.claim_chunk` |
| Poison pill + `reject_on_worker_lost` | A task that always crashes its child is requeued forever. | `attempts` counter in PostgreSQL; broker-side `x-delivery-limit` exists only on quorum queues |
| `request.retries` is blind | It counts `self.retry()` calls, not crashes or redeliveries. | Retry budget lives in the DB, not in Celery |
| `Reject(requeue=False)` tells the chord nothing | No result is stored, so the chord waits forever. | The task marks the call FAILED in PostgreSQL itself |
| Dead-lettering to a missing exchange | RabbitMQ **silently drops** the message. We hit this while building (the DLQ peek 404'd). | `app/bootstrap.py` declares `dlx` + `dead_letter` |
| Queue arguments are immutable | Changing `x-dead-letter-exchange` later → `PRECONDITION_FAILED`. | `make reset` |
| Hard time limit ACKs | A task killed by `task_time_limit` is *not* redelivered. | Soft limit (30 s) fires first and retries |
| Time limits need the prefork pool | They silently do nothing on solo/threads/gevent. | Documented in `config.py` |
| `result_expires` vs chord state | Chord bookkeeping lives in the result backend and expires with it; it must outlive your slowest workflow. | `config.py` |
| RabbitMQ `consumer_timeout` (30 min default) | With late acks, a task running longer than this has its channel closed by the broker. Relevant for long ETL backfills. | Not triggered here (chunks last seconds) |
| Redis as the *broker* + `visibility_timeout` (1 h default) | Unacked messages are redelivered after an hour, so a long late-ack task gets duplicated. | We use RabbitMQ; Redis is only the result backend |
| ETA / countdown tasks sit in worker memory | A retry with `countdown=60` is held by a worker (and counts against its prefetch) until due. | Keep countdowns short; see `backoff_seconds` |
| Prefetch (default 4 × concurrency) | With RabbitMQ the reserved messages are *not lost* on a crash (they are unacked, so requeued); the damage is **unfair dispatch**: a busy worker hoards messages while a neighbour idles. | Experiment 3 |

**Use it when** tasks are short or idempotent, the Python ecosystem matters, and you can accept
"at-least-once, tune it yourself". **Be careful when** tasks are long, run on preemptible hardware, and
a lost task is a data-correctness or revenue problem. Then Celery is workable, but *you* are the
reliability layer.

---

## 2. Redis as a task queue

The "legacy" shape from your meeting: long-lived workers pulling from Redis structures.

**Why it feels bulletproof.** Redis is fast and its commands are atomic. On stable on-demand machines
workers rarely die, so the sharp edges never show. That is stability by *environment*, not by design.

**The naive version loses work:** `BRPOP queue` removes the item *before* you process it. A crash
between pop and finish = lost. The classic fix is the **reliable-queue pattern**:

```text
BLMOVE queue processing RIGHT LEFT 0     # atomically move item to a per-worker "processing" list
... do the work ...
LREM processing 1 <item>                 # acknowledge
```

(`RPOPLPUSH`/`BRPOPLPUSH` are the same idea, deprecated since Redis 6.2 in favour of `LMOVE`/`BLMOVE`.)
You then need a sweeper that moves stale items back, which is a Task Police by another name.
**Redis Streams** do this properly: `XADD`, `XREADGROUP`, `XACK`, and `XAUTOCLAIM` to steal work from
dead consumers. That is essentially what `acks_late` + the lease in this repo emulates.

**What raw Redis does not give you** for this pipeline: fan-in (the chord), retries with backoff,
a dead-letter policy, routing, rate limits, time limits, a dashboard. You would rebuild each one, and
a *dependency graph* ("merge after all N chunks") is where hand-rolled queues stop scaling: you end up
writing a workflow engine one counter at a time. Also note Redis durability is configurable and
asynchronous (RDB/AOF, async replication), so a failover can lose recently acknowledged work.

**Use it when** the queue is simple (independent jobs), volume is modest, and you want minimal infrastructure.

---

## 3. Kafka with Python

**Kafka is an event log, not a task queue.** Producers append records to partitions; consumers in a
group track an **offset** per partition. Nothing is "acked per message": you commit how far you have read.

```python
from confluent_kafka import Consumer

c = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "chunk-analyzers",
              "enable.auto.commit": False, "auto.offset.reset": "earliest"})
c.subscribe(["chunks"])
while True:
    msg = c.poll(1.0)
    if msg is None or msg.error():
        continue
    analyze(msg.value())     # 5-10 s of work
    c.commit(message=msg)    # "everything up to here is done"
```

Why that is awkward for *tasks*:

* **Parallelism is capped by partition count.** One partition is consumed by one group member at a time.
* **Head-of-line blocking.** A slow or failing record blocks every record behind it in its partition.
  There is no "skip this one, retry it later" primitive.
* **No delayed or per-message retry.** You build retry topics (`chunks.retry.5s`, `chunks.retry.1m`) and
  a dead-letter topic by hand.
* **Long work fights the group protocol.** If you do not call `poll()` within `max.poll.interval.ms`
  (5 min default) the consumer is evicted and the partition rebalances, which can redeliver in-flight work.
* **No fan-in.** "Run the merge when all 8 chunks are done" means state in a database or a stream
  processor. You are writing the chord yourself.

Where Kafka wins: very high throughput, **replay** (re-process last week's calls after fixing a bug),
many independent consumers of the same stream, ordering per key, and event-driven architectures.
A sensible split for a call platform: Kafka carries *"call ended"* events; a task system or workflow
engine does the multi-step processing those events trigger.

---

## 4. Temporal workflows

Temporal inverts the model. You do not queue tasks and hope; you write the pipeline as ordinary code and
the server **records every step in an event history** so it can resume the code exactly where it stopped.

This repo's pipeline in Temporal (conceptual; `pip install temporalio`):

```python
import asyncio
from datetime import timedelta
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError


@activity.defn
async def ingest_call(call_id: str) -> list[str]:
    ...  # validate, create chunk rows, return chunk ids


@activity.defn
async def analyze_chunk(chunk_id: str) -> dict:
    for tick in range(ticks):
        await asyncio.sleep(1)
        activity.heartbeat(tick / ticks)           # replaces our DB heartbeat + lease
    if corrupt:
        raise ApplicationError("corrupt audio", non_retryable=True)   # replaces PermanentError -> DLQ
    return result


@activity.defn
async def merge_and_finalize(call_id: str, results: list[dict]) -> dict:
    ...


@workflow.defn
class CallPipeline:
    @workflow.run
    async def run(self, call_id: str) -> dict:
        chunk_ids = await workflow.execute_activity(
            ingest_call, call_id, start_to_close_timeout=timedelta(seconds=30))

        results = await asyncio.gather(*(                       # replaces chord(group(...))
            workflow.execute_activity(
                analyze_chunk, chunk_id,
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(seconds=10),         # replaces lease + Task Police
                retry_policy=RetryPolicy(                        # replaces retry/backoff/attempts column
                    initial_interval=timedelta(seconds=2), backoff_coefficient=2.0,
                    maximum_interval=timedelta(seconds=60), maximum_attempts=4),
            )
            for chunk_id in chunk_ids
        ))

        return await workflow.execute_activity(
            merge_and_finalize, args=[call_id, results], start_to_close_timeout=timedelta(seconds=30))
```

### What this removes from the Celery version

| Celery scaffolding in this repo | Temporal equivalent |
|---|---|
| `acks_late`, `reject_on_worker_lost` | Not a concept. The server, not a broker message, owns "this activity must complete". A worker that dies stops heartbeating; after `heartbeat_timeout` the server reschedules the activity on another worker. |
| Lease + fencing token + heartbeat column | `activity.heartbeat()` and `heartbeat_timeout`, enforced server-side |
| `attempts` column, `backoff_seconds`, poison-pill cap | `RetryPolicy(maximum_attempts=..., backoff_coefficient=...)` |
| `chord`, `self.replace()`, error callback | `asyncio.gather` in the workflow body: plain Python control flow, dynamic fan-out included |
| `Task Police` + `begin_recovery` + stall query | Nothing. A workflow cannot "stall silently": every step is a durable event with a timer behind it. |
| Chord result in Redis + `result_expires` | Workflow history |
| `prefetch_multiplier` | Workers *pull* from a task queue only when they have a free slot (`max_concurrent_activities`), so there is nothing to hoard |
| Flower + SQL on `task_execution_log` | Temporal Web UI shows the full history of every workflow |
| DLQ via `Reject(requeue=False)` | There is no separate DLQ. A non-retryable failure fails the activity; your workflow handles it, or the workflow fails and is visible, resettable and re-runnable |

### What Temporal does **not** remove (be honest about this)

* **Activities are at-least-once.** A worker can die after the side effect but before reporting
  completion, so the activity runs again. Idempotency matters exactly as much as here
  (use the activity/workflow ID as an idempotency key for external calls).
* **Workflow code must be deterministic** (no `random`, no wall-clock, no direct I/O). It is replayed
  from history, so changing a running workflow's code needs *versioning*.
* **History has limits.** Very large fan-outs or very long-lived workflows need `Continue-As-New`.
* **Operational cost.** A Temporal cluster (frontend/history/matching services) plus a database, vs a
  single RabbitMQ container. That is why teams reserve it for workflows where dropping work is
  unacceptable (billing, call processing with strict SLAs) and keep Celery for fire-and-forget jobs.

---

## 5. Choosing

* **Short, independent, idempotent jobs; Python team; low ceremony** → Celery (+ RabbitMQ), with the
  reliability trio set deliberately.
* **Trivial queue, tiny scale** → Redis Streams, but know you are signing up to build the rest.
* **High-volume event streams, replay, many consumers** → Kafka; pair it with a task or workflow system for the multi-step work.
* **Multi-step, long-running, must-not-lose, stateful business processes** → Temporal.

The most useful number from this project: count the lines in `repository.py` and `tasks/police.py` that
exist only to compensate for a message being a weak promise. That is the Celery tax for this workload,
and what Temporal's pitch is to make unnecessary.
