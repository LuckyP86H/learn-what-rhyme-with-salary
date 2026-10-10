# learn-what-rhyme-with-salary
Max self-learning Celery with joy

## Projects

| Directory | What it is |
|---|---|
| [`call-pipeline/`](call-pipeline/README.md) | A call-intelligence pipeline (ingest → parallel chunk analysis via a Celery chord → merge) with a Spot-eviction chaos simulator, built to learn Celery's reliability behaviour by breaking it. Start with `cd call-pipeline && make up && make trigger`. |
| [`call-pipeline/COMPARISON.md`](call-pipeline/COMPARISON.md) | Celery vs Redis vs Kafka vs Temporal, using that pipeline as the yardstick. |
