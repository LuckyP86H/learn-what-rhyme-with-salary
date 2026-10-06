# Call Pipeline: learning Celery by breaking it

A small but realistic **call-intelligence pipeline** built to learn Celery's reliability behaviour the
hard way: by killing workers mid-task and watching what survives. Everything runs locally in Docker
Compose, at no cost.

```
POST /calls ──▶ ingest_call ──▶ chord( group( analyze_chunk × N ) ──▶ merge_and_finalize )
   API           default q        heavy_processing q (the "Spot fleet")      default q
```

| Piece | Tech | Role |
|---|---|---|
| Broker | RabbitMQ 3.13 | Delivers task messages; ack/requeue/dead-letter semantics are what we study |
| Result backend | Redis 7 | Celery results + chord bookkeeping |
| Durable state | PostgreSQL 15 | The workflow's source of truth (`calls`, `call_chunks`, `task_execution_log`) |
| API | FastAPI | Starts calls, exposes state, peeks at the DLQ |
| Workers | Celery 5.6 | `worker-spot` (×2, kills target) · `worker-core` (ingest, merge, police) · `beat` |
| Observability | Flower, JSON logs | http://localhost:5555 |

## Quick start

Needs Docker (Docker Desktop or OrbStack) and `make`. No Python install needed on the host.

```bash
make up           # build + start everything (first run pulls images)
make trigger      # one 5-chunk call, watched to completion (~25 s)
make help         # every command
```

| URL | What |
|---|---|
| http://localhost:8100/docs | API (port **8100**, not 8000, which is often taken; override with `API_PORT=...`) |
| http://localhost:5555 | Flower: live tasks and workers |
| http://localhost:15672 | RabbitMQ UI (`pipeline` / `pipeline`): watch *Ready* vs *Unacked* per queue |

## How the pipeline works

1. **`POST /calls`** inserts a `calls` row, then publishes `tasks.ingest_call(call_id)`.
2. **`ingest_call`** validates the call, creates the chunk rows, and *replaces itself* with the chord
   (`self.replace(chord(group(analyze_chunk…), merge_and_finalize))`). The textbook
   `chain(ingest | chord(group(...)))` can't be written up front because the chunk ids don't exist until
   ingest creates them.
3. **`analyze_chunk`** (5–10 s each) first **claims** its chunk in PostgreSQL, then heartbeats while it
   "works". The claim makes it idempotent: finished chunk → return the stored result; live owner →
   wait; dead owner (heartbeat older than the lease) → take over.
4. **`merge_and_finalize`** is the chord callback. It aggregates from the *database* and marks the call
   `COMPLETED` exactly once. A second delivery is a logged no-op.
5. **No `.get()`, no `allow_join_result`.** No worker ever blocks waiting for another task.

State machines: call `PENDING → INGESTED → PROCESSING → COMPLETED | FAILED`;
chunk `PENDING → PROCESSING → COMPLETED | FAILED`.

## Celery vocabulary, as used here

| Term | Meaning in this repo |
|---|---|
| **Task** | A Python function run by a worker (`app/tasks/pipeline.py`) |
| **Broker** | RabbitMQ: holds messages until a worker takes them |
| **Ack** | The worker telling the broker "done, you can forget this message". *When* it does so is the crux of everything below |
| **Result backend** | Redis: stores task return values and the chord's bookkeeping |
| **Prefetch** | How many messages a worker reserves in advance |
| **Canvas** | `chain` (A then B), `group` (A, B, C in parallel), `chord` (group, then one callback) |
| **`self.replace`** | A task swaps itself for a workflow computed at runtime |
| **`Reject(requeue=False)`** | Refuse a message *without* putting it back; with a DLX configured, RabbitMQ parks it in the dead-letter queue |
| **Soft / hard time limit** | Soft raises an exception inside the task so it can clean up; hard SIGKILLs the process |
| **beat** | Celery's scheduler: publishes the Task Police sweep periodically |

The three settings that decide whether work survives a worker dying
(full explanations are in [`config.py`](config.py)):

```python
task_acks_late = True              # ack AFTER the task finishes, not when it starts
task_reject_on_worker_lost = True  # if the pool child dies, requeue instead of dropping
worker_prefetch_multiplier = 1     # don't hoard messages behind a long task
```

## Experiments

Each experiment is one command and prints a verdict. Run them in this order.

### 1. Hardened vs stock Celery under a Spot eviction

```bash
make test-spot            # hardened: acks_late + reject_on_worker_lost + prefetch 1
make test-spot-default    # Celery's out-of-the-box settings
```

The simulator starts a call with 8 chunks, waits until half are done and one is mid-flight, evicts the
worker running it, boots a replacement 5 s later, then answers three questions from the audit trail.
Eviction styles (`MODES="sigkill child term-kill sigterm" make test-spot`):

| Mode | What dies | What it teaches |
|---|---|---|
| `sigkill` | the whole container, instantly | Broker connection drops → requeue only works with `acks_late` |
| `term-kill` | SIGTERM, then SIGKILL 3 s later | A realistic Spot interruption notice that's too short to finish |
| `sigterm` | warm shutdown only | Celery finishes in-flight work and exits: graceful, **not** an eviction |
| `child` | only the pool child process | The parent stays connected, so RabbitMQ sees nothing: this is what `reject_on_worker_lost` is for |

Everything else is available for poking:
`python3 app/chaos/spot_killer.py --help` (add `--pause` to step through it).

### 2. Fair dispatch: `worker_prefetch_multiplier`

```bash
make test-prefetch
```

Runs a call with two slow chunks under multiplier 4, then 1, printing RabbitMQ's *ready* / *unacked*
counts, each consumer's `prefetch_count`, and which worker ran which chunk.

### 3. The failure catalogue

Each fault exercises exactly one Celery feature:

```bash
make trigger-retry     # transient error → exponential backoff; hang → soft time limit. Ends COMPLETED
make trigger-bad       # corrupt audio → PermanentError → Reject → dead-letter queue. Ends FAILED
make trigger-poison    # chunk kills its own process → requeued until the attempts cap → DLQ
make trigger-bug       # unexpected exception → task FAILURE → chord error callback → call FAILED
make dlq               # peek at parked messages
make events ID=<call_id>   # the audit trail behind any of the above
```

Or craft your own: `POST /calls {"num_chunks": 6, "faults": {"1": "transient", "4": "slow"}}`.

### 4. The Task Police

`tasks/police.py` runs every 60 s (beat) and re-dispatches calls with no heartbeat for 120 s. Under
hardened settings it has nothing to do. Under stock settings (experiment 1) it is the only thing that
rescues the lost chunk. Read its docstring, then `COMPARISON.md` for why this pattern exists.
`make police-now` triggers a sweep on demand.

## Observed results

> Measured on this repo (OrbStack, Celery 5.6.3, RabbitMQ 3.13). Timings vary run to run.

**Experiment 1: eviction of a worker mid-chunk (8 chunks, kill at 4 done)**

| Config | Eviction | Chunk in flight | Call outcome |
|---|---|---|---|
| hardened | `sigkill` | **Re-queued** by RabbitMQ (`redelivered=True`); a surviving worker took it over after the 10 s lease expired | COMPLETED by itself; callback fired once |
| hardened | `term-kill` (2 s grace) | Same: cut off, re-queued, taken over | COMPLETED by itself |
| hardened | `child` | Parent worker survived; `reject_on_worker_lost` requeued it; **same container** finished it | COMPLETED by itself |
| hardened | `sigterm` | Warm shutdown: finished normally, then the container exited | COMPLETED; not a loss scenario |
| **default** | `sigkill` | **Dropped**: message was ACKed at start, so RabbitMQ had nothing to requeue. Call sat **stuck** | COMPLETED only after the Task Police noticed and re-dispatched that one chunk in a new chord (~75 s) |
| **default** | `child` | `WorkerLostError`; task FAILED | Call **FAILED loudly** via the chord error callback (a different failure mode from the silent stall above) |

Two things worth staring at in the default/`sigkill` run: only the *one running* chunk was lost
(the other chunks, though prefetched, were requeued by the broker), and it was silent until the police
acted. Nothing in Celery noticed.

A *duplicate delivery* showed up for free in one `term-kill` run: the worker was killed after storing a
result but before ACKing, so RabbitMQ redelivered a finished chunk. The claim check logged
`CHUNK_SKIPPED_ALREADY_DONE` and moved on. That is why every task here is idempotent.

**Experiment 2: prefetch (8 chunks, chunks 0 and 2 slow, two 1-process workers)**

| `worker_prefetch_multiplier` | RabbitMQ *unacked* at start | Who ran what | Idle time | Total |
|---|---|---|---|---|
| 4 | **8** (each worker reserved 4) | A: `c0* c2* c4 c6`, B: `c1 c3 c5 c7` | **B idle ~30 s** while A ground through slow chunks | **61 s** |
| 1 | **2** (only what is running), 6 still *ready* | A: `c0* c3 c5 c7`, B: `c1 c2* c4 c6` | ≤ 4 s | **49 s** |

With 4, the work was pre-assigned round-robin before anyone knew chunks 0 and 2 were slow. With 1,
whichever worker was free took the next message.

**Experiment 3: the fault catalogue**

| Fault | Result |
|---|---|
| `transient` | Failed twice, retried with backoff (≈2 s, ≈4 s), succeeded on attempt 3. Chord still completed |
| `hang` | Soft time limit (30 s) fired, chunk retried and finished. Chord completed |
| `permanent` | Call FAILED; message in `dead_letter` with `reason: rejected` |
| `crash` | Pool child SIGKILLed 4× and requeued each time by `reject_on_worker_lost`; attempts cap stopped the poison pill; call FAILED; message in the DLQ |
| `bug` | Task FAILURE → `ChordError` → error callback marked the call FAILED |

## Precision notes (where common explanations of Celery need correcting)

These came up while building and *testing* this project, and are worth knowing because the usual
folklore gets them wrong:

1. **`reject_on_worker_lost` is not what saves you from a container kill.** A killed container drops its
   AMQP connection, and RabbitMQ requeues unacked messages automatically. That needs only `acks_late`.
   `reject_on_worker_lost` covers a *pool child* dying while the parent survives (OOM killer).
2. **Prefetched messages are not lost when a worker dies on RabbitMQ.** They are unacked, so they are
   requeued (visible in experiment 1 with prefetch 4: the other chunks survive). The real cost of
   prefetch is *unfair dispatch*: experiment 2.
3. **`docker kill -s SIGTERM` is not a Spot eviction.** Celery turns SIGTERM into a warm shutdown and
   finishes the running task. A real eviction is SIGTERM *then* power-off (`term-kill`), or just power-off (`sigkill`).
4. **`chain(ingest | chord(group(chunk_ids)))` cannot be built up front** when ingest creates the
   chunks. Use `self.replace(...)`.
5. **`Reject` never reaches the chord.** A rejected task stores no result, so the chord just waits.
   Failure must also be recorded in the database. Treat the DB as truth, the chord as a trigger.
6. **Dead-lettering into a missing exchange silently drops the message.** Declare the topology
   yourself (`app/bootstrap.py`); no consumer will do it for you.
7. **Lease TTL is your failover latency.** After a kill, the replacement waits for the dead worker's
   10 s lease to expire before taking over. Shorter = faster recovery, more risk of duplicate work.

## Layout

```
call-pipeline/
├── docker-compose.yml        # rabbitmq, redis, postgres, bootstrap, api, worker-spot x2, worker-core, beat, flower
├── Dockerfile  requirements.txt  Makefile
├── config.py                 # env parsing, reliability profiles, queues, Celery config (read the comments!)
├── database.py               # SQLAlchemy models: Call, CallChunk, TaskExecutionLog
├── app/
│   ├── main.py               # FastAPI
│   ├── celery_app.py         # Celery instance + signals
│   ├── repository.py         # every DB state transition: claim / heartbeat / complete / finalize / recovery
│   ├── analysis.py           # pure, deterministic fake transcription + aggregation
│   ├── bootstrap.py          # DB schema + RabbitMQ exchanges/queues (incl. dead-letter topology)
│   ├── logging_config.py     # JSON logs with task context
│   ├── tasks/pipeline.py     # ingest_call, analyze_chunk, merge_and_finalize, error callback, fault injection
│   ├── tasks/police.py       # the Task Police
│   └── chaos/                # spot_killer.py, prefetch_demo.py (host-side, stdlib only)
├── scripts/                  # run_test_pipeline.sh, test_spot_failure.sh
├── tests/                    # `make test`
└── COMPARISON.md             # Celery vs Redis vs Kafka vs Temporal
```

## Troubleshooting

* **Port already in use**: the API is on 8100 by default. `API_PORT=8200 make up` to change it.
* **`PRECONDITION_FAILED` in worker logs**: queue arguments changed since the queues were declared. `make reset && make up`.
* **Nothing happens after `make trigger`**: `make logs` and `make ps`; workers need ~5 s after `make up`.
* **Stack left in the wrong mode?** `test_spot_failure.sh` switches modes and restores hardened mode when it
  exits. If it was killed hard, run `make up` (or `curl localhost:8100/config` to see the mode in effect).

## Known limitations (deliberate)

* If a call FAILs, its other in-flight chunks keep running to completion (wasted work, harmless).
* The API publishes after committing the DB row (no transactional outbox); a failed publish leaves a
  `PENDING` call that the police recover.
* Classic RabbitMQ queues, single node. Quorum queues (`x-delivery-limit`, replication) are the natural next step.
* Schema via `create_all`, not Alembic.
