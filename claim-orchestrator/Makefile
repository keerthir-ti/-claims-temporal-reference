# Everything runs in Docker; the host needs only Docker and make.
SHELL   := /bin/bash
COMPOSE := docker compose
SIM     := $(COMPOSE) --progress quiet run --rm --no-deps simulator

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs demo scenario tasks decide history archive loadtest test test-java test-python

help: ## List targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env

up: .env ## Build and start Temporal, Postgres, the workers and the event feed; make the build current
	$(COMPOSE) up -d --build --wait
	$(COMPOSE) --progress quiet run --rm promote-worker-versions
	@source .env; echo; echo "  Temporal UI  http://localhost:$${TEMPORAL_UI_HOST_PORT:-28233}"; echo

down: ## Stop (data kept)
	$(COMPOSE) --profile tools --profile setup down

clean: ## Stop and delete all data
	$(COMPOSE) --profile tools --profile setup down -v --remove-orphans

ps: ## Service status
	$(COMPOSE) ps -a

logs: ## Follow logs (S=service)
	$(COMPOSE) logs -f --tail 100 $(S)

demo: ## Run every scenario in mock-data/scenarios end to end and check the outcomes
	$(SIM) run

scenario: ## Run one scenario (N=02-injury-retriage)
	$(SIM) run $(N)

tasks: ## Open adjuster tasks
	$(SIM) tasks

decide: ## Decide a task (T=TASK-1001 C=APPROVE AS=sup-2 ROLES=payment-approver [AUTH=5000000])
	$(SIM) decide $(T) $(C) --as $(AS) --roles $(ROLES) --authority $(or $(AUTH),0)

history: ## Business history of a claim (ID=...)
	$(SIM) history $(ID)

archive: ## Run the nightly archive now for claims closed more than DAYS days ago (default 0)
	$(SIM) archive --days $(or $(DAYS),0)

loadtest: ## Catastrophe surge: CLAIMS claims at RATE per second, all must close once with a complete record
	$(SIM) load --claims $(or $(CLAIMS),100) --rate $(or $(RATE),10)

test: test-java test-python ## All tests

test-java: ## Java tests (workflow tests with the time-skipping test server)
	docker build -q -f workers/lifecycle-worker/Dockerfile --target test .
	docker build -q -f workers/stp-worker/Dockerfile --target test .

test-python: ## Python tests
	docker build -q -f workers/triage-worker/Dockerfile --target test .
	docker build -q -f workers/investigation-worker/Dockerfile --target test .
