# celery_viz

A **read-only** monitor that explains what a Celery workflow is doing in terms of Celery's own
concepts. It listens to the event stream (`task-*`, `worker-*`), folds it into runs, and serves the
result over HTTP + Server-Sent Events. It never publishes a task and never touches your code.

> Status: core, collector, API, replay, and the React UI (served at `/` by the same process).

## Run it

```bash
make up                                  # the demo stack includes a `viz` service on :8200
curl localhost:8200/api/health           # connected? how many events seen?
curl localhost:8200/api/runs             # newest runs first
curl -N localhost:8200/api/stream        # live feed (SSE)

# no broker needed: replay a recording (also how the tests work)
python -m celery_viz --replay tests/viz/fixtures/celery5_pipeline.jsonl --speed 4

# point it at any Celery deployment's broker
python -m celery_viz --broker amqp://user:pass@host:5672// --lost-after 10
```

The workers being observed must emit events: `worker_send_task_events=True` (or `celery worker -E`),
and for the *queued* stage `task_send_sent_event=True` on the publishers.

## API

| Endpoint | What |
|---|---|
| `GET /api/health` | mode, counts, collector status (`connected`, `events_seen`, `last_error`), `dropped_events` |
| `GET /api/states` | the state table the UI renders (`states.json`) |
| `GET /api/runs?limit=` | run summaries, newest first |
| `GET /api/runs/{id}` | one run: tasks (with timelines), edges, derived states |
| `GET /api/workers` | workers with `online` / `offline` / `lost` and their own heartbeat period |
| `GET /api/stream` | SSE: one `snapshot`, then `run` (full view of any changed run) and `workers` |

Streaming is **snapshot + delta**, so a reconnecting browser needs no replay. The server also
re-evaluates still-active runs every second, because some states (worker lost, ETA reached) are the
*absence* of an event and nothing arrives to announce them.

## How states are described

Every state is an entry in [`states.json`](states.json): label, the Celery state/event it maps to,
a plain-language explanation, and links to the settings that change it. The UI renders that file; no
state is special-cased in code. A test fails if a future Celery adds a state the table does not describe.

Some states are **derived** (the monitor's inference, labelled as such): `REPLACED`, `ENDED_SILENTLY`,
`WAITING_ETA`, `DRAINING`, `WORKER_LOST`, `SUSPECTED_HUNG`.

## Things Celery's events do not tell you (all measured, all covered by tests)

* **A task id is not unique.** `self.replace()` reuses it under a new name, and the original emits no
  terminal event. Tasks are keyed by `(uuid, generation)`.
* **A SIGKILLed worker emits nothing**, not even `worker-offline`. "Lost" is inferred from silence,
  using each worker's own heartbeat period (`freq`; the docs' "every minute" is not what workers do).
* **`worker-offline` is not "dead".** It is sent at the *start* of a warm shutdown (SIGTERM), while
  in-flight tasks keep running. That is `DRAINING`; it becomes lost only after `--drain-grace`.
* **Workers interleave full and short heartbeats.** Only the full ones carry `active`; its value is
  meaningful only as of the moment it was reported.
* **No event carries `redelivered`.** A redelivery is a repeated `task-received` with the same `retries`.
* **Chord fan-in is not in the data** (the callback has one parent); extra join edges are inferred and marked.
* **Events are lossy.** The collector must keep one consumer open: re-subscribing on idle auto-deletes
  the event queue and drops whatever is published in the gap (an earlier version lost ~1 event in 20).
* **The ack is not observable.** `acks_late` can be explained, not measured, from events.

## Redaction and security

* Event payloads (`args`, `kwargs`, `result`, `traceback`) are **dropped before storage** by default
  (`--keep-args`, `--keep-result` opt in). Exception text is kept, truncated.
* The API has **no authentication**: it binds to loopback by default; put a proxy with auth in front
  of it before exposing it.
* It is read-only by construction: there is no endpoint that sends, revokes or purges anything.

## Compatibility

| | Tested |
|---|---|
| Monitor on Celery 5.6.3, workers on 5.6.3 / RabbitMQ 4.2, 4.3, 4.4 | yes (live + recorded) |
| Monitor 5.6.3 reading a **Celery 4.4.7** fleet's events | yes (recorded): same fields; chains reconstruct |
| 4.4.7: chord headers submitted directly by a client | **not groupable by `root_id`** (each header is its own root); use a single parent task or a run-key rule |
| Redis as the broker, other Celery versions | **not verified** |

On RabbitMQ 4.3+ the monitor's own event queue must be exclusive (it is: `event_queue_exclusive`);
see [docs/rabbitmq4-deprecations.md](../docs/rabbitmq4-deprecations.md).

## Validate against ground truth

```bash
python3 scripts/check_viz.py        # submits calls, then compares the monitor with the database
```

## Layout

```
states.json    the state vocabulary (data)         store.py      events -> runs (facts only)
states.py      loader + validation                 derive.py     read-time guesses + views
redaction.py   strip payloads                      collector.py  broker listener / replay driver
config.py      thresholds                          server.py     FastAPI + SSE
model.py       data holders                        __main__.py   CLI
```
