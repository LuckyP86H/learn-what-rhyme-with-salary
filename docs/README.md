# Documentation

Each page does one job, so you always know where to look and where to write.

| Kind | Page | Use it to |
|---|---|---|
| **Learn** | [Tutorial](tutorial.md) | Follow a guided path of experiments, each with what to do and what to watch |
| **Understand** | [Concepts](concepts.md) | Learn the Celery ideas (acks, prefetch, canvas, events) and where folklore is wrong |
| | [Pipeline](pipeline.md) | See how the call pipeline works and why it is built the way it is |
| | [Architecture](architecture.md) | See the services and data flows, and what is reused instead of rebuilt |
| | [Visualizer](visualizer.md) | Read the screen, use the launcher, point it at your own project |
| | [Comparison](comparison.md) | Compare Celery, Redis, Kafka and Temporal |
| | [RabbitMQ 4 and Celery](rabbitmq4-deprecations.md) | Understand a compatibility risk and how to mitigate it |
| **Do** | [How-to](how-to.md) | Run a recipe: other broker version, quorum queues, your own project, dev checks, troubleshooting |
| **Look up** | [Reference](reference/) | Find a state, API route, flag, `make` target, service, variable or source file |
| **Measure** | [Results](results.md) | See dated measurements and how to re-measure them |

## Keeping the docs current

The pages in [`reference/`](reference/) are **generated**: never edit them. They are produced from the
code, the Compose file, the Makefile and `states.json`, so they cannot drift from reality. The rest is
hand-written, and tests fail when it mentions something that no longer exists.

| You changed | Update | How |
|---|---|---|
| A task state (`celery_viz/states.json`) | nothing by hand | `make docs`; the UI reads the same file |
| An API route, CLI flag, `make` target, Compose service, port or environment variable | nothing by hand | `make docs` |
| Added or renamed a source file | give it a one-line header comment | `make docs` refreshes the [source map](reference/modules.md) |
| A fault or demo scenario | its table in [Pipeline](pipeline.md) or [Visualizer](visualizer.md) | `make docs-check` fails until it is documented |
| Something you measured | [Results](results.md) | re-run the command shown there, update the numbers and the date |
| A Celery idea you learned or a belief you had to correct | [Concepts](concepts.md) | add it where it belongs; link to it instead of repeating it |

Before you commit documentation or code that affects it:

```bash
make docs         # regenerate the reference pages
make docs-check   # fail if they are stale, or if a page mentions a make target, file or port that does not exist
```

### Conventions

- **No file trees.** They go stale on the next rename. The generated [source map](reference/modules.md) replaces them.
- **Numbers live in [Results](results.md)** and nowhere else, each with a date and a command to reproduce it. Elsewhere, link to it.
- **Link, don't copy.** If a fact has a home (a state, a setting, a port), link to it.
- Diagrams are [Mermaid](https://mermaid.js.org/), which GitHub renders and which is plain text, so changes show up in diffs.
