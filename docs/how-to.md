# How-to

Short recipes. Every command here also appears in [reference/make.md](reference/make.md) or
[reference/env.md](reference/env.md).

## Run on another broker version

```bash
RABBITMQ_VERSION=4.2 make reset up
```

The default is RabbitMQ 4.3. `make reset` is needed because queue arguments cannot change on an existing broker.

## Run on quorum queues

```bash
QUEUE_TYPE=quorum make reset up
```

Quorum queues are the long-term direction for RabbitMQ and Celery; they change how prefetch and delayed tasks work.
What changes, and what it costs, is in [RabbitMQ 4 and Celery](rabbitmq4-deprecations.md#6-quorum-queues-measured-with-their-price).

## Point the visualizer at another Celery deployment

See [Visualizer: use it with your own project](visualizer.md#use-it-with-your-own-project).

## Add a demo scenario

1. Write the tasks in `app/tasks/` (see `app/tasks/meeting.py` for plain examples) and give each a route in `config.py`
   (a test fails if a task has no route).
2. Add an entry to `SCENARIOS` in `app/viz_catalog.py` and handle its id in `DemoCatalog.launch`. Declare its parameters
   in the entry; they are validated before launch.
3. If the scenario must appear as **one run**, start it from a parent task (see `translate_all`) rather than submitting
   several tasks from the client.
4. Document it in the [Visualizer](visualizer.md#the-demo-scenarios) table (`make docs-check` fails until its id appears there).

## Run the visualizer read-only

```bash
VIZ_CATALOG= make up
```

The launch panel then says "Read-only mode" and the API refuses launches. See [Visualizer](visualizer.md#the-demo-scenarios).

## Describe a new task state

Edit `celery_viz/states.json`, then `make docs`. Pick a `station` and `tone` that already exist, or add a `tone`
colour in `viz-ui/src/styles.css`.

## Validate the monitor against the database

```bash
make viz-check              # submits calls, then compares the monitor with the database
CALLS=4 CHUNKS=2 make viz-check
```

Run it after any change to the collector or reducer. It exists because some bugs only appear against a live broker.

## Develop

Everything runs in containers, so the host needs only Docker and `make`.

```bash
make test           # Python tests, against your working tree
make lint           # Ruff import ordering (needs a local venv, see below)
make ui-test        # UI typecheck and unit tests
make ui             # rebuild the UI into celery_viz/ui_dist
make docs-check     # generated docs fresh, and no stale references in the hand-written ones
```

`make test`, `make docs` and `make docs-check` run your working tree inside the project image, so after changing
`requirements.txt` run `docker compose build` once to refresh the image's dependencies.

After `make ui` you can see UI changes without rebuilding the image:

```bash
docker cp celery_viz/ui_dist/. call-pipeline-viz-1:/srv/celery_viz/ui_dist/
```

For hot reload with a local Node 24: `cd viz-ui && npm ci && npm run dev` (it proxies `/api` to the running `viz` service).

For `make lint` and editor support, create a local environment once:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
```

Ruff enforces sorted imports. For VS Code, install the Ruff extension and add to your workspace settings
(`.vscode` is gitignored here, so this stays local):

```json
{ "[python]": { "editor.codeActionsOnSave": { "source.fixAll.ruff": "explicit", "source.organizeImports.ruff": "explicit" } } }
```

CI runs Ruff, the Python tests, and the UI typecheck, tests and build on every push.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| "Port already in use" | Every host port can be changed; the variable is listed in [reference/services.md](reference/services.md), e.g. `API_PORT=8200 make up` |
| `PRECONDITION_FAILED` in worker logs | Queue arguments changed since the queues were created: `make reset && make up` |
| Nothing happens after `make trigger` | `make ps` and `make logs`; workers need about 5 seconds after `make up` |
| The visualizer says *reconnecting* | The `viz` service is down: `make ps`, then `make logs` |
| The visualizer shows no workers | Events are off on the workers (`-E`), or the monitor cannot reach the broker: `curl localhost:8200/api/health` shows `source.connected` and `last_error` |
| Workers are "Up" but nothing runs (RabbitMQ 4.3) | Celery's control and event queues are refused; see [RabbitMQ 4 and Celery](rabbitmq4-deprecations.md) |
| A Redis client on the default port 6379 shows the wrong data | A native Redis owns that port and shadows the container; the container is on **6380** |
| The stack is in the wrong mode after an experiment | `make up` restores hardened mode; `curl localhost:8100/config` shows the mode in effect |
