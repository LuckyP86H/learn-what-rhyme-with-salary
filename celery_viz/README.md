# celery_viz

A read-only monitor that explains what a Celery workflow is doing in Celery's own terms. It listens to the
event stream, folds it into runs, and serves a web UI and a small HTTP/SSE API. It never imports the code it
watches and never publishes a task (launching demo scenarios is opt-in).

```bash
python -m celery_viz --broker amqp://user:pass@host:5672//     # live, against any Celery deployment
python -m celery_viz --replay recording.jsonl                  # no broker needed
```

Everything else lives in the main docs, so there is one place to update:

- [How to read the screen, use it with your own project, and its known limits](../docs/visualizer.md)
- [What Celery's events do and do not tell you](../docs/concepts.md#what-celerys-events-do-and-do-not-tell-you)
- Generated reference: [states](../docs/reference/states.md), [HTTP API](../docs/reference/api.md), [command-line flags](../docs/reference/cli.md), [source map](../docs/reference/modules.md)
