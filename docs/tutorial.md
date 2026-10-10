# Tutorial: learn Celery by breaking it

A guided path. Each step says what to **do**, what to **watch**, and **why** it happens. Steps build on
each other, so go in order the first time. Every step uses the visualizer at http://localhost:8200
unless it says otherwise. A key to the colours and states is always at the bottom of the screen; click
any state there for its explanation.

Before you start: `make up` (see the [README](../README.md#run-it)).

## 1. One task

**Do:** in *Launch a workflow*, open **Single task** and press *Launch*.

**Watch:** the ball starts in *Broker queue*, moves to *Reserved by a worker*, then *Running* (with a
live timer) and ends in *Outcome* as **Succeeded**. Click the task's row: the right panel shows its
timeline and, for every state, whether Celery **reports** it or this monitor **infers** it.

**Why:** this is the whole Celery lifecycle. A message waits in the broker, a worker reserves it, a
pool process runs it, and the result is recorded. See [Concepts: the task lifecycle](concepts.md#the-task-lifecycle).

## 2. Three shapes

**Do:** launch **Chain**, then **Group**, then **Chord** (leave the fault at *none*).

**Watch:** the banner above the lanes names the shape and counts how many tasks ran at once.

| Scenario | Banner says | What it means |
|---|---|---|
| Chain | *Chain of 3* | each task starts only after the previous one succeeds |
| Group | *3 tasks at the same time* | independent tasks, nothing joins them |
| Chord | *N in parallel, then one join* | a callback runs once **all** of them have succeeded |

Two things to notice in the chord. While it is still running the banner can say *"No join seen yet"*,
because Celery only announces the join callback after the last task succeeds. And the parent
`ingest_call` ends as **Replaced**: it handed itself over to the chord with `self.replace()`, which
emits no "finished" event. See [Concepts: what events do not tell you](concepts.md#what-celerys-events-do-and-do-not-tell-you).

## 3. A retry

**Do:** launch **Single task** with *Make one task misbehave* = `transient`.

**Watch:** the ball runs, fails, goes **Retrying**, returns to the queue as **Queued** (a *retry ×1* chip
appears), sits in **Waiting for ETA**, runs again, and so on for two retries before it succeeds.

**Why:** a retry is a **new message** for the same task id with an ETA of now plus a backoff delay. The
growing waits come from `retry_backoff`. Click the **Retrying** pill in the legend for the details.

## 4. A failure, two ways

**Do:** launch **Single task** with `permanent`. Then launch **Chord** with `permanent`.

**Watch:**

- The single task ends **Failed**: an ordinary exception, Celery's `FAILURE`.
- In the chord the bad chunk ends **Rejected**, the parent shows **Ended silently**, and the run is
  **FAILED**. Look at the queue panel: the `dead_letter` queue gains a message.

**Why:** the pipeline deliberately uses `Reject(requeue=False)` for errors that can never succeed, so
RabbitMQ parks the message in the dead-letter queue instead of looping. A rejected task never stores a
result, so the chord is never told and the join simply never runs. That is why the database, not the
chord, is the source of truth here. See [Pipeline](pipeline.md#failures-on-purpose).

## 5. A task that hangs

**Do:** launch **Chord** with `hang`.

**Watch:** one chunk runs for a long time and turns **Suspected hung**, then Celery's *soft time limit*
(30 s) interrupts it, it is retried, and it succeeds.

**Why:** Celery has no per-task heartbeat, so a hung task and a slow one look identical; "suspected hung" is
this monitor's guess after a threshold. The soft limit is what actually rescues it.

## 6. A poison pill

**Do:** launch **Chord** with `crash`.

**Watch:** the chunk bounces between **Queued** and **Running** several times, then ends **Rejected**
and the run **FAILED**.

**Why:** the task kills its own pool process every time. With `task_reject_on_worker_lost` the message is
requeued each time, which would loop forever; the attempts counter in the database stops it after 4 tries
and dead-letters it.

## 7. A worker dies

This is the experiment the whole project exists for.

**Do:** launch a **Chord** with 4 chunks. While chunks are running, kill **both** Spot workers, wait about
ten seconds, then bring them back:

```bash
docker kill -s SIGKILL $(docker ps -q --filter label=com.docker.compose.service=worker-spot)
# ...about ten seconds later:
docker compose up -d --no-deps worker-spot
```

**Watch:** after a few seconds the running chunks turn **Worker lost** with a pulsing ring, and the
*Workers* panel shows both workers `lost`. When the workers return, the chunks come back as **Running**
with a **redelivered** chip, and the run completes.

**Why:** a killed worker sends nothing, so "lost" is inferred from missing heartbeats. The work survives
because `task_acks_late = True`: RabbitMQ never saw an acknowledgement, so it put the messages back on the
queue. You can see that setting's live value in the inspector when you click the **Running** pill.

Now repeat the same idea with the terminal tools and compare **hardened** against **Celery's stock settings**:

```bash
make test-spot           # hardened: the chunk is re-queued and the call completes by itself
make test-spot-default   # stock settings: the chunk is dropped; only the Task Police rescue the call
```

With stock settings the original run in the visualizer stays in **ATTENTION** with a chunk stuck at
*Worker lost*, because nothing in Celery will ever bring that message back. The Task Police's recovery
appears as a separate, newer run. The measured numbers are in [Results](results.md#eviction-of-a-worker-mid-chunk).

## 8. A polite shutdown is not an eviction

**Do:**

```bash
python3 app/chaos/spot_killer.py --mode sigterm
```

**Watch:** the running chunk turns **Draining**, finishes, and succeeds. Nothing is lost.

**Why:** Celery treats SIGTERM as a *warm shutdown*: it announces `worker-offline`, stops taking new work,
and lets running tasks finish. This is why a deploy is safe and a Spot eviction (SIGKILL) is not. Tasks the
worker had only *reserved* are never started; they go back to the queue.

## 9. Fair dispatch

**Do:** `make test-prefetch`

**Watch:** it runs the same workload with `worker_prefetch_multiplier` set to 4, then 1, and prints how many
messages each worker reserved, who ran what, and how long each worker sat idle. The queue panel in the
visualizer shows the same thing live as the **unacked** count.

**Why:** with a high multiplier a worker reserves messages it cannot start yet, so a neighbour can sit idle
while work waits behind a slow task. See [Concepts: prefetch](concepts.md#prefetch).

## 10. Change a setting, predict the result

Pick one of these, **write down what you expect to see**, then run it and compare:

```bash
RELIABILITY_MODE=default docker compose up -d --force-recreate worker-spot worker-core
CELERY_ACKS_LATE=false docker compose up -d --force-recreate worker-spot worker-core
CELERY_PREFETCH_MULTIPLIER=4 docker compose up -d --force-recreate worker-spot worker-core
```

Then repeat step 7. The effective values show up in the inspector (open any state that lists settings).
Every variable you can change is in [reference/env.md](reference/env.md). Put the stack back with `make up`.

## Where next

- [Concepts](concepts.md): the ideas behind everything you just saw.
- [Visualizer](visualizer.md): what each part of the screen means, and how to point it at your own project.
- [Results](results.md): how these experiments measured out, and how to re-measure them.
