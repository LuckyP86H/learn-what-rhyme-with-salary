# Measured results

The **only** page that contains measurements, so there is one place to refresh when something changes.
Everything else links here.

> **Measured on 2026-10-10** with Python 3.14.5, Celery 5.6.3, RabbitMQ 4.2.9 and 4.3.6 (classic and quorum
> queues), on OrbStack. Timings vary from run to run; the *pattern* is what repeats. When you re-measure,
> update the numbers **and** this date.

## Eviction of a worker mid-chunk

Re-measure: `make test-spot` and `make test-spot-default` (add `MODES="sigkill child term-kill sigterm"` to try
every eviction style). 8 chunks; the worker running a chunk is killed once half are done.

| Config | Eviction | Chunk in flight | Call outcome |
|---|---|---|---|
| hardened | `sigkill` | **Re-queued** by RabbitMQ (`redelivered=True`); a surviving worker took it over after the 10 s lease expired | Completed by itself; callback fired once |
| hardened | `term-kill` (2 s grace) | Cut off, re-queued, taken over | Completed by itself |
| hardened | `child` | Parent worker survived; `reject_on_worker_lost` requeued it; the **same container** finished it | Completed by itself |
| hardened | `sigterm` | Warm shutdown: finished normally, then the container exited | Completed; not a loss scenario |
| **default** | `sigkill` | **Dropped**: the message was acknowledged at start, so there was nothing to requeue. The call sat stuck | Completed only after the Task Police re-dispatched that one chunk, about 85 s |
| **default** | `child` | `WorkerLostError`, task failed | **Failed loudly** through the chord's error callback (a different failure from the silent stall above) |

Two things worth staring at in the default `sigkill` run: only the **one running chunk** was lost (the other
chunks, though prefetched, were requeued by the broker), and it was **silent**: nothing in Celery noticed until
the police acted.

A *duplicate delivery* appeared for free in one `term-kill` run: the worker died after storing a result but
before acknowledging it, so RabbitMQ redelivered a finished chunk. The claim check logged
`CHUNK_SKIPPED_ALREADY_DONE` and moved on, which is why every task here is idempotent.

## Fair dispatch

Re-measure: `make test-prefetch`. 8 chunks, chunks 0 and 2 slow, two workers of one process each.

| `worker_prefetch_multiplier` | *Unacked* at start | Who ran what | Idle time | Total |
|---|---|---|---|---|
| 4 | **8** (each worker reserved 4) | A ran `c0* c2* c4 c6`, B ran `c1 c3 c5 c7` | **B idle about 34 s** while A worked through the slow chunks | **61 s** |
| 1 | **2** (only what is running), 6 still *ready* | A ran `c0* c3 c5 c6`, B ran `c1 c2* c4 c7` | 3 s or less | **47 s** |

(`*` marks a slow chunk.) With 4 the work was assigned before anyone knew which chunks were slow. The exact
split varies per run; the idle worker does not.

## The failure catalogue

Re-measure: launch each fault from the visualizer, or `make trigger-retry`, `make trigger-bad`,
`make trigger-poison`, `make trigger-bug`. What the **visualizer** showed:

| Fault | Scenario | States the task passed through | Run ended |
|---|---|---|---|
| `transient` | single task | Queued, Running, Queued, Waiting for ETA, Running, Queued, Waiting for ETA, Running, Succeeded (2 retries) | completed |
| `permanent` | single task | Queued, Reserved, Running, **Failed** | failed |
| `permanent` | chord | chunk **Rejected**; parent *Ended silently* | failed; `dead_letter` gained a message |
| `crash` | chord | chunk bounced between Queued and Running (redelivered), then **Rejected** after the attempts cap of 4 | failed; message in the DLQ |
| `hang` | chord | Running, **Suspected hung** (after 20 s), Queued, Running, Succeeded | completed (soft limit at 30 s) |
| `transient` | chord | as the single task, for one chunk | completed |
| `bug` | chord | task failure, then the chord's error callback marks the call failed | failed |

## The visualizer against ground truth

Re-measure: `make viz-check`. The monitor was compared with the database: **45 of 45 chunks across 15
concurrent calls** matched, after fixing a collector bug that had been losing about 1 terminal event in 20.

Killing **both** Spot workers mid-run (the [tutorial](tutorial.md#7-a-worker-dies) recipe): at about 14 s two chunks
showed **Worker lost** with the attention ring and the Workers panel listed both workers lost after about 7 s of
silence; after the workers returned the chunks showed *Running* with a *redelivered* chip and the run completed.

The ball's motion was measured, not assumed: sampled every 100 ms, its position moved 779, 783, 817, 852, 879,
890, 895, 897 px over roughly 700 ms (an ease-out curve) when a task went from Running to Succeeded.

## Scale of the visualizer

Re-measure: `make bench-viz` (no broker needed). Synthetic events, 2026-10-10.

| What | Measured |
|---|---|
| Event ingestion | about 450,000 events per second |
| One pass over **500 runs of 10 tasks** (the server does this once a second) | about 10 ms, roughly 1% of a core |
| One run with **1,500 parallel tasks** | about 4 ms to render, 3,001 edges |

This was a real bug before it was a benchmark: chord-join inference scanned every task for every task, so a
single run with 2,000 parallel tasks took **467 ms** per view, recomputed every second while holding the store lock
(which also blocks the event collector). It is now linear; a regression test fails if it ever becomes quadratic again.

The store caps a run at 2,000 tasks and drops further events for it (counted in `dropped_events` on `/api/health`),
so one runaway workflow cannot take the monitor down. In the browser, a run past 100 tasks draws the first 100 plus
every task that failed or needs attention, and folds the rest into a summary with a *Show all* button (checked on a
synthetic 301-task run with two failures at positions 250 and 280).

## Redis as the broker

Re-measure: this is a manual check (no `make` target): run Redis, a Celery 5.6.3 worker with `-E`, and
`python -m celery_viz --broker redis://...` on one network, then submit a chain. Verified 2026-10-10 in an isolated stack:

| Check | Result |
|---|---|
| Monitor connects and receives events | yes |
| Worker seen with its software and 2 s heartbeat period | yes |
| A chain of 3 tasks | one run, 3 tasks, 2 `spawned` edges, all Succeeded |
| A task submitted on its own | its own run |
| Worker killed with SIGKILL | shown **lost** after about 12 s of silence |
