COMPOSE ?= docker compose
API_PORT ?= 8100
POSTGRES_PORT ?= 5432
API ?= http://localhost:$(API_PORT)
CHUNKS ?= 5
export API_PORT POSTGRES_PORT API

.DEFAULT_GOAL := help
.PHONY: help up down reset logs ps wait trigger trigger-retry trigger-bad trigger-poison trigger-bug status events dlq config \
        lint lint-fix test-spot test-spot-default test-prefetch viz-check ui ui-test docs docs-check test flower rabbitmq police-now psql

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## build + start the whole stack in hardened mode
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait
	@echo ""
	@echo "  API       $(API)/docs"
	@echo "  Flower    http://localhost:5555"
	@echo "  RabbitMQ  http://localhost:15672   (pipeline / pipeline)"
	@echo "  Viz API   http://localhost:8200/api/health"
	@echo "  PostgreSQL jdbc:postgresql://localhost:$(POSTGRES_PORT)/pipeline?user=pipeline&password=pipeline"
	@echo ""
	@echo "Next: make trigger"

down: ## stop everything (keeps the database volume)
	$(COMPOSE) down

reset: ## stop everything and DELETE all state (db volume, queues)
	$(COMPOSE) down -v

wait: ## block until the API answers
	@echo "waiting for the API..."; \
	for i in $$(seq 1 60); do curl -fs $(API)/health >/dev/null && echo "API is up" && exit 0; sleep 2; done; \
	echo "API did not come up; try: make logs"; exit 1

logs: ## follow worker + beat logs (JSON)
	$(COMPOSE) logs -f --tail=50 worker-spot worker-core beat

ps: ## show containers
	$(COMPOSE) ps

trigger: ## run one 5-chunk call end-to-end and watch it finish (CHUNKS=8 to change)
	./scripts/run_test_pipeline.sh $(CHUNKS)

trigger-retry: ## recoverable faults: chunk 1 fails twice (backoff retry), chunk 3 hangs (soft time limit). Ends COMPLETED
	./scripts/run_test_pipeline.sh 4 '{"1":"transient","3":"hang"}'

trigger-bad: ## chunk 2 has corrupt audio (PermanentError): call ends FAILED, message parked in the DLQ (make dlq)
	./scripts/run_test_pipeline.sh 5 '{"2":"permanent"}' || true

trigger-poison: ## chunk 2 kills its own process every time (poison pill): requeued until the attempts cap, then DLQ
	./scripts/run_test_pipeline.sh 5 '{"2":"crash"}' || true

trigger-bug: ## chunk 2 raises an unexpected exception: task FAILURE -> chord error callback -> call FAILED
	./scripts/run_test_pipeline.sh 5 '{"2":"bug"}' || true

status: ## list recent calls
	@curl -s $(API)/calls | python3 -m json.tool

events: ## audit trail of a call: make events ID=<call_id>
	@curl -s $(API)/calls/$(ID)/events | python3 -m json.tool

dlq: ## peek at the dead-letter queue
	@curl -s $(API)/dead-letters | python3 -m json.tool

config: ## show the reliability settings in effect
	@curl -s $(API)/config | python3 -m json.tool

test-spot: ## Spot-eviction chaos test against HARDENED Celery (expect: call completes)
	./scripts/test_spot_failure.sh hardened

test-spot-default: ## same test against Celery's STOCK defaults (expect: work lost, call stuck until the police act)
	./scripts/test_spot_failure.sh default

test-prefetch: ## prefetch_multiplier=4 vs 1: watch workers hoard messages vs share fairly
	./scripts/test_spot_failure.sh prefetch

lint: ## check Python import ordering with Ruff
	ruff check .

lint-fix: ## automatically organize Python imports with Ruff
	ruff check --fix .

viz-check: ## validate the visualizer against the database (submits calls; ~3 min, or CALLS=4 CHUNKS=2)
	python3 scripts/check_viz.py --calls $(or $(CALLS),15) --chunks $(or $(CHUNKS),3)

ui: ## build the visualizer UI into celery_viz/ui_dist (in a Node container; nothing installed on the host)
	docker run --rm -u "$$(id -u):$$(id -g)" -e HOME=/tmp -v "$$PWD":/repo -w /repo/viz-ui node:24-slim sh -c "npm ci --no-audit --no-fund && npm run build"

ui-test: ## UI typecheck + unit tests (in a Node container)
	docker run --rm -u "$$(id -u):$$(id -g)" -e HOME=/tmp -v "$$PWD":/repo -w /repo/viz-ui node:24-slim sh -c "npm ci --no-audit --no-fund && npm run typecheck && npm test"

test: ## Python unit tests, run against the working tree (not the copy baked into the image)
	$(COMPOSE) run --rm --no-deps -T -v "$$PWD":/srv api python -m pytest -q -p no:cacheprovider

docs: ## regenerate docs/reference/* from the code, config and compose file
	$(COMPOSE) run --rm --no-deps -T -v "$$PWD":/srv api python scripts/gen_docs.py

docs-check: ## fail if generated docs are stale or hand-written docs mention things that no longer exist
	$(COMPOSE) run --rm --no-deps -T -v "$$PWD":/srv api python -m pytest -q -p no:cacheprovider tests/test_docs.py

police-now: ## run one police sweep immediately instead of waiting for beat
	$(COMPOSE) exec worker-core celery -A app.celery_app:celery_app call tasks.police_sweep

flower: ## open Flower
	open http://localhost:5555

rabbitmq: ## open the RabbitMQ management UI
	open http://localhost:15672

psql: ## psql shell into PostgreSQL
	$(COMPOSE) exec postgres psql -U pipeline pipeline
