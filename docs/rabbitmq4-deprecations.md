# RabbitMQ 4 is deprecating things Celery uses: what it means and what to do

**Short version:** two RabbitMQ features that Celery relies on by default are on a deprecation path.
RabbitMQ 4.2 only warns. **RabbitMQ 4.3 already refuses one of them, and with stock Celery settings
every worker silently stops consuming while its process stays "Up".** A two-line config change fixes
it. The other feature (prefetch via "global QoS") still worked in every test I ran, but it is the one
to watch, and quorum queues are the long-term path.

Everything below marked **(measured)** was run in this repo against real brokers (Celery 5.6.3,
Python 3.14.5, RabbitMQ 4.2.9 and 4.3.6). Everything marked **(docs)** comes from vendor documentation
and was not independently verified. Anything I could not establish is labelled as such.

## 1. What the two features are

| RabbitMQ feature | Plain meaning | What in Celery uses it |
|---|---|---|
| `transient_nonexcl_queues` | A queue that is **non-durable *and* non-exclusive**: it vanishes on broker restart, yet any connection may use it | Celery's **control/pidbox** queues (`<node>.celery.pidbox`, the "mingle" step at worker start, `celery inspect/control`) and **event** queues (`celeryev.*`: Flower, monitors) |
| `global_qos` | `basic.qos(global=true)`: one prefetch limit shared across a whole channel instead of per consumer | How Celery implements `worker_prefetch_multiplier` on RabbitMQ (`celery/worker/consumer/tasks.py`) |

Neither is exotic. They are Celery defaults, so any Celery-on-RabbitMQ deployment uses both.

## 2. Timeline

| Fact | Source |
|---|---|
| Both deprecated since August 2021; the official list gives **no removal version** | (docs) RabbitMQ "List of Deprecated Features" |
| 4.2.9: both **permitted**, broker logs a warning | (measured) |
| 4.3.6: `transient_nonexcl_queues` is **refused** (`INTERNAL_ERROR - Feature ... is deprecated. By default, this feature is not permitted anymore`) | (measured) |
| 4.3.6: broker's own table lists `global_qos` as `denied_by_default`, yet `basic.qos(global=true)` + consume on a classic queue was **accepted** in direct tests and in all worker runs. One refusal line appeared once in a broker log during a restart; I could not attribute or reproduce it | (measured; unresolved) |
| Broker warning text: "will be removed from a future major RabbitMQ version... actual version to be determined" | (measured) |
| `rabbitmq:4-management` already floats to 4.3, so anyone on the moving tag got this without choosing it | (measured) |

## 3. What actually happens on 4.3 with stock Celery (measured)

Reproduced with this project's config minus the fix, i.e. Celery 5.6.3 defaults:

* Workers connect, then die at "mingle: searching for neighbors", reconnect in a tight loop and end with
  `CRITICAL ... RestartFreqExceeded('5 in 1s')`.
* **The container stays "Up".** All queues show `consumers=0`. Nothing consumes a task. A liveness check
  that only asks "is the process running?" stays green.
* Flower shows no workers; `celery inspect ping` fails with the same error.
* Downstream: `beat` kept publishing expiring `police_sweep` messages that nobody consumed; they
  expired into the dead-letter queue (`reason: expired`). The outage left a fingerprint in the DLQ.

## 4. How big is the impact?

### This project

* Before the fix: **total outage on 4.3** (nothing runs), **no impact on 4.2** (warnings only).
* After the fix: none on 4.2/4.3, verified with the full chaos matrix (see section 6).
* Remaining exposure: `global_qos` (prefetch). Not failing today; it is the next candidate.

### A plausible industry Celery project

I can't know your numbers, so here is the shape of the risk and what decides its size:

* **The trigger is a broker upgrade, not a code deploy.** Nothing in your repo changes, tests pass,
  and the failure appears when the broker moves to 4.3+. On a managed service (Amazon MQ, CloudAMQP, ...)
  that is on the provider's schedule; self-hosted, on yours.
* **Blast radius is everything at once:** every worker on that broker, plus Flower and any
  `inspect`/`control` tooling.
* **It fails quietly** (section 3), so detection depends on queue-depth or consumer-count alerts, not
  process health.
* **Cost to fix is tiny if known in advance, high if discovered during an incident.**
* Likelihood by broker version: 3.x, none. 4.0-4.2, warnings only. 4.3+, outage with defaults.
* Other AMQP clients that declare non-durable, non-exclusive queues (home-grown `pika` consumers,
  other frameworks) are affected the same way; Celery is just the most common case.
* **Not verified:** other Celery versions. I tested 5.6.3, where `control_queue_exclusive` and
  `event_queue_exclusive` exist. Check that your version has them before relying on the fix.

Reasonable calibration: *not an emergency, but a known-date-unknown cliff*, and cheap to defuse now.

## 5. Mitigations, in order

1. **Inventory (30 minutes).** Broker type and version, who controls upgrades, Celery version, any
   non-Celery AMQP consumers. If the broker is below 4.3 and pinned, you have time; if it's on a
   floating tag or managed auto-upgrade, you do not.
2. **Make Celery's queues exclusive.** Two settings, already in `config.py`:
   ```python
   control_queue_exclusive = True   # pidbox: each node's control queue is used only by that node
   event_queue_exclusive = True     # celeryev: each receiver's queue is used only by that receiver
   ```
   Verified to remove all refusals on 4.3.6 for workers, `inspect`, and Flower. Flower must run with
   the same Celery config (here it now runs from the project image instead of `mher/flower`, which
   knows nothing about these settings).
3. **Test against the new broker before it reaches production.** CI/staging job that runs the real
   worker against `rabbitmq:4.3-management` (here: `RABBITMQ_VERSION=4.3 make up`). RabbitMQ documents
   `deprecated_features.permit.<name> = false` for simulating removal early *(docs; I did not run this
   switch since 4.3 already denies by default)*.
4. **Alert on consumers, not processes.** "Consumer count on queue X is 0 for N minutes" would have
   caught section 3.
5. **Plan the move to quorum queues (section 6).** It removes the dependence on `global_qos`
   altogether and is what both RabbitMQ and Celery document as the direction.

## 6. Quorum queues: measured, with their price

`QUEUE_TYPE=quorum RABBITMQ_VERSION=4.3 make up` (fresh broker required: queue type is immutable).

Full chaos matrix on RabbitMQ 4.3.6 + quorum queues, all as expected (measured):

| Scenario | Result |
|---|---|
| Hardened, sigkill / child / term-kill | Re-queued with `redelivered=True`, callback fired once |
| Default settings, sigkill | Chunk dropped; recovered only by the Task Police |
| `permanent`, `crash`, `bug` faults | Correct outcomes; DLQ holds only `rejected` messages |
| Prefetch 4 vs 1 | Unfairness still visible (57 s vs 47 s) |

What changes, and what I found along the way:

| Change | Detail |
|---|---|
| Prefetch becomes static | Celery logs `Global QoS is disabled. Prefetch count in now static.` **(docs)**: no autoscaling, `worker_enable_prefetch_count_reduction` is a no-op. We use neither |
| **`direct` exchanges break ETA tasks** | With quorum queues, Celery uses *native delayed delivery*, which refuses `direct` exchanges and then **blocks a worker slot until the ETA arrives** (warning logged). Our retry backoff uses ETA. Fix: `topic` exchanges (now used everywhere; exact keys route identically) |
| Delayed-delivery infrastructure | Celery creates 28 `celery_delayed_*` quorum queues |
| Control/event queues stay classic | They remain non-durable but exclusive, hence allowed. The section 5 fix is still required |
| `confirm_publish` | Enabled for quorum so a publish means "durably accepted" **(docs recommend)** |
| Migration is not in place | Queue type and exchange type can't be changed on existing queues. Plan blue/green or drain-and-recreate |

**Not measured:** throughput/latency, memory and disk cost of quorum vs classic, multi-node behaviour.
This is a single-node lab, which is the place to learn the semantics, not to size production.

## 7. Recommended next step

For a real project: do steps 1 to 4 now (hours, not days, and they remove the cliff). Treat quorum
queues as a *planned migration* with its own staging soak, not a reaction. For this repo: default is
RabbitMQ 4.3 + classic queues with the fix; quorum is one env var away for experiments.

## 8. Reproduce

```bash
make reset && make up                                  # RabbitMQ 4.3, classic, with the fix
RABBITMQ_VERSION=4.2 make reset up                     # same on 4.2
QUEUE_TYPE=quorum make reset up                        # quorum on 4.3
make test-spot                                         # chaos matrix against whichever you started
```

To see the failure itself: remove the two `*_queue_exclusive` lines from `config.py`, run on 4.3, and
watch `docker compose logs worker-core` and `rabbitmqctl list_queues name consumers`.
