# `acks_late` in practice: where this repo really depends on it

**Short answer to "is it used for real here?"** Yes, but not as a `@task(acks_late=True)` decorator.
A search of the repo finds **no per-task override**. It is one *global* switch, `task_acks_late`, set in
[`config.py`](../config.py) by the `hardened` profile (`True`) and the `default` profile (`False`, Celery's
stock value). Every task inherits it, including ones that never asked for it. That detail matters (see
"The part that is easy to get wrong").

Nothing below is new measurement; the experiment results are in the README and `make test-spot*` reproduce
them. This page explains *why* the setting exists and how to rehearse the scenario yourself.

## The scenario: a Spot instance is reclaimed mid-chunk

`tasks.analyze_chunk` ([`app/tasks/pipeline.py`](../app/tasks/pipeline.py)) takes 5-10 s per audio chunk on
a worker that can disappear with two minutes' notice (or none). The chord only fires when **every** chunk
reports back.

| | `acks_late=False` (Celery default) | `acks_late=True` (hardened) |
|---|---|---|
| When RabbitMQ is told "done with this message" | the moment the worker **starts** the task | after the task **returns** |
| Worker is SIGKILLed 4 s into a chunk | Broker already considers the message delivered and finished. **Chunk is gone**, chord never completes | The message is still unacknowledged, the connection drops, RabbitMQ **requeues** it, another worker runs it |
| What you pay | nothing, and you can lose work | the task can run **more than once**, even at the same time |

So the setting trades "might lose work" for "might do work twice". Neither is free; which one is right
depends on what the task does.

## The part that is easy to get wrong

`acks_late` alone is not a fix, it is a *transfer of responsibility* to your code:

* **`analyze_chunk` earns it.** It is written for double delivery: `repository.claim_chunk` takes a row lock,
  answers `ALREADY_DONE` for finished chunks, `BUSY` while another worker holds a live lease, and takes over
  only when the owner's heartbeat is older than the lease. A redelivery is harmless by design. Without that
  code, `acks_late=True` would just turn lost work into duplicated work.
* **`Reject(requeue=False)` only works because of it.** The permanent-failure path (corrupt audio) rejects the
  message so it dead-letters to the DLQ. With early acks the message is already gone, so there would be
  nothing to reject (see the comment in `analyze_chunk`).
* **The `tasks.meeting_*` tasks inherit it too and have no idempotency machinery** (their module says so).
  They are simulations, so a duplicate is harmless here. In a real service a task like "send the summary
  e-mail" under a global `acks_late=True` could send twice after a worker crash. In practice that is the
  reason to prefer a per-task override (`@app.task(acks_late=True)`) for the few tasks that need it
  and are idempotent, rather than a global switch. This repo uses the global switch because every task
  that matters to the experiment is the idempotent one; do not copy that blindly.
* **It does not cover a dead pool child.** If only the child process is OOM-killed, the parent keeps the broker
  connection, so RabbitMQ sees nothing. That is a different setting, `task_reject_on_worker_lost`, and it
  only does anything together with `acks_late`.

## How to get familiar with it (do these in order)

1. **See the failure with stock Celery.**
   ```bash
   make test-spot-default
   ```
   The worker is killed mid-chunk, the work is lost and the call stays stuck until the Task Police
   re-dispatch it. That stall is exactly what `acks_late=False` costs.
2. **See the fix.**
   ```bash
   make test-spot
   ```
   Same kill, but the chunk is redelivered and the call completes. The audit trail
   (`make events ID=<call_id>`) shows the redelivery being handled by the lease.
3. **Change only `acks_late`, keeping the rest of the hardened profile.** `config.py` lets an env var override
   a single setting, and compose passes it through:
   ```bash
   RELIABILITY_MODE=hardened CELERY_ACKS_LATE=false make up
   ```
   Then run the kill again. Comparing this with step 2 tells you what `acks_late` alone contributes versus
   `reject_on_worker_lost` and prefetch. (Rehearse with a *sigkill* of the whole container versus a kill of
   the pool child; the two settings protect against different deaths.)
4. **Watch it live.** In the visualizer ([http://localhost:8200](http://localhost:8200)) open a running task:
   the queue panel shows the cluster's current `task_acks_late` value next to the broker's unacked count. With `True` the unacked count stays up for the whole run; with `False` it drops as soon as tasks
   start. Events cannot show the acknowledgement itself, so the broker metric is the evidence.
5. **Ask the production question about your own tasks.** For each task: if it ran twice, would anything
   visible happen (money, e-mail, an external API call, a counter)? If yes, either make it idempotent first
   or leave it on early ack and accept the loss risk. Write that decision down next to the task.

## Where it shows up in this repo

| Place | What it does with the setting |
|---|---|
| [`config.py`](../config.py) | defines both profiles and the env override (`CELERY_ACKS_LATE`) |
| [`app/tasks/pipeline.py`](../app/tasks/pipeline.py) | `analyze_chunk` is the idempotent consumer; the `Reject` path depends on it |
| [`app/chaos/spot_killer.py`](../app/chaos/spot_killer.py) | the experiment that asks "did acks_late + reject_on_worker_lost prevent data loss?" |
| [`celery_viz/remote_config.py`](../celery_viz/remote_config.py) | reads the live value from the running workers; mixed values across workers are flagged |
| [`COMPARISON.md`](../COMPARISON.md) | how other systems (Kafka offsets, workflow engines) answer the same question |
