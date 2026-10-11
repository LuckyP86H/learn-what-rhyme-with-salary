# The workflow visualizer

A read-only monitor that explains what a Celery workflow is doing **in Celery's own terms**. Open
http://localhost:8200 after `make up`.

It is not a replacement for Flower or Prometheus. Those answer "is the fleet healthy?"; this answers "what
happened to *this* workflow, and why?". See [Architecture](architecture.md#what-is-reused-and-what-is-not).

## Reading the screen

| Area | What it shows |
|---|---|
| **Launch a workflow** (left) | Demo scenarios you can start; only appears when launching is enabled |
| **Runs** (left) | Every workflow seen, newest first, with its status. A *run* is a root task and everything it spawned |
| **Workers** (left) | Which workers are online, shutting down, or lost. Workers silent for over 5 minutes are folded away |
| **Banner** (top of the middle) | The workflow's shape (single, chain, parallel, or fan-out then join) and how many tasks ran at once |
| **Lanes** (middle) | One row per task. A ball moves along the row as the task changes state. Click a row to inspect it |
| **Queues** (middle, below) | The broker's own count of messages *ready* (waiting) and *unacked* (handed to a worker, not yet acknowledged) |
| **Inspector** (right) | What the selected state means in Celery terms, the Celery state and event behind it, and the **live value** of every setting that affects it |
| **Legend** (bottom) | Every state the monitor can show. Click one to read what it means; filled pills occur in the selected run |

### The four stages

The columns are the stages of a task's life; the **ball's column is the task's current stage**.

| Stage | Meaning |
|---|---|
| Broker queue | Stored by the broker, waiting for a worker |
| Reserved by a worker | A worker took the message but has not started it |
| Running | A pool process is executing it |
| Outcome | How this attempt ended |
| Lost | No worker is known to hold it any more |

### Reading a ball

- **Colour** comes from the state's `tone` in `states.json`; the label next to the ball always says the state
  in words, so colour is never the only signal.
- **A dashed outline** means the state is *inferred* by this monitor; Celery does not report it.
- **A pulsing ring** means the state needs a human (for example *Worker lost* or *Suspected hung*).
- **Chips** beside a task name: *retry ×N* (it was retried), *redelivered* (the broker handed it to another
  worker after the first one disappeared), *duplicate ×N* (it ran again after already succeeding).
- Motion respects the operating system's *reduce motion* setting.

Every state, whether Celery reports it or this monitor infers it, and the settings that change it are listed in
[reference/states.md](reference/states.md).

## The demo scenarios

Launch them from the left panel. Each can make one task misbehave, which is how the
[tutorial](tutorial.md) produces retries, rejections and hangs.

| Scenario | Id | Shape you should see | Notes |
|---|---|---|---|
| Single task | `single_task` | single | One plain task |
| Chain | `chain` | chain of 3 | transcribe, then summarize, then extract action items |
| Group | `group` | 3 tasks at the same time | a parent publishes three translations; nothing joins them |
| Chord | `chord` | N in parallel, then one join | the hardened call pipeline, with 1 to 12 chunks |

The fault options for the chord are the [pipeline's faults](pipeline.md#failures-on-purpose); the plain scenarios
support `none`, `transient`, `permanent` and `slow`.

Launching is a **switch**, and it is on in the demo stack. Only the named scenarios can run, their parameters are
validated, and there is no way to send an arbitrary task or arguments. For a purely read-only monitor:

```bash
VIZ_CATALOG= make up      # the catalog reports itself disabled and POST /api/launch returns 404
```

A monitor started on its own (`python -m celery_viz`) has launching **off** unless you pass `--catalog` or set `CELERY_VIZ_CATALOG`.

## Known limits

- **A chord looks like a group until it finishes.** Celery announces the join callback only after the last task
  succeeds, so while running the banner says *"No join seen yet"* instead of guessing.
- **Inferred states are guesses.** *Worker lost*, *Draining*, *Ended silently* and *Suspected hung* come from
  missing or contradictory events, and are marked as such. *Ended silently* can flicker for about a second when
  several tasks start at the same moment on one worker.
- **A task recovered within seconds is never shown as lost.** If the broker requeues it before the silence
  threshold, you see only a *redelivered* chip. The [tutorial](tutorial.md#7-a-worker-dies) kills both Spot workers to
  make the lost state last long enough to see.
- **Per-task settings are invisible.** The inspector shows the *global* value of a setting; a task decorated with
  its own `acks_late` can differ. The value is flagged when workers disagree.
- **When the ack happens is not observable from events**; the queue panel shows the broker's measured counts instead.
- **Events are lossy.** The monitor keeps one consumer open to avoid dropping events, but a broker restart can lose some.
- **Very large runs are folded.** Past 100 tasks the UI draws the first 100 plus *every* task that failed or needs attention, and
  summarises the rest with a *Show all* button.
- **Not verified:** a real screen-reader pass (rows are keyboard-focusable and have accessible names, but that is not the same thing).

## Use it with your own project

The monitor only needs the broker URL; it never imports your code.

1. **Turn events on** in the workers you want to watch: start them with `-E`, or set `worker_send_task_events=True`.
   To also see the *Queued* stage, set `task_send_sent_event=True` on whatever publishes tasks.
2. **Run it** against the broker (see [reference/cli.md](reference/cli.md) for every flag):

   ```bash
   python -m celery_viz --broker amqp://user:pass@host:5672// --host 127.0.0.1
   ```
3. **Group related tasks.** Runs are grouped by the *root* task. Fan out from a parent task (as `ingest_call` does)
   so a workflow shares one root; tasks submitted straight from a client (a bare `group` or `chord`) each get their
   own root and appear as separate runs.
4. **Mind the data.** Task arguments, results and tracebacks are dropped before storage unless you pass
   `--keep-args` or `--keep-result`. The API has **no authentication**: keep it on loopback or put an authenticating
   proxy in front.
5. **Silence noise** such as a periodic sweep with `--ignore-task NAME`.

| Setup | Status |
|---|---|
| Monitor and workers on Celery 5.6.3, RabbitMQ 4.2 and 4.3 | verified live |
| Monitor 5.6.3 reading a Celery 4.4.7 fleet | verified from recorded events: same fields, chains reconstruct; chord headers submitted directly by a client are not groupable |
| Redis as the broker (events, chain edges, lost-worker detection) | verified live in an isolated stack |
| Other Celery versions | **not verified** |

## Extending it

- **Describe or add a state:** edit `celery_viz/states.json`. The UI, the inspector and the generated
  [states page](reference/states.md) all read it. A test fails if a future Celery adds a state it does not describe.
- **Add a demo scenario:** see [How-to](how-to.md#add-a-demo-scenario).
- **Tune thresholds:** `--lost-after`, `--drain-grace`, `--hung-after` (see [reference/cli.md](reference/cli.md)).
