# Everything runs in Docker: no Python, Java or Maven needed on the host. Only Docker + make.
#   make up       start the stack           make q1 … q7  our answer to each of the 7 design questions, live
#   make demo     all 6 scripted claims     make help     every target
SHELL   := /bin/bash
COMPOSE := docker compose
RUN     := $(COMPOSE) --progress quiet run --rm --no-deps demo
CLI     := $(RUN) python -m demo.cli

# Fresh claim ids per demo run, e.g. make seed FIXTURE=CLM-1002 -> CLM-1002-153012
SUFFIX  := $(shell date +%H%M%S)
FIXTURE ?= CLM-1001
ID      ?=
CLAIMS  ?= 300
RATE    ?= 30

need = $(if $($(1)),,$(error $(1) is required, e.g. make $@ $(1)=$(2)))

.DEFAULT_GOAL := help
.PHONY: help up down clean ps logs urls pilot full q1 q2 q3 q4 q5 q6 q7 questions demo seed state progress \
        patch decide ledger reopen cancel-payment history actions loadtest test test-python test-java elk-up elk-down

help: ## List targets
	@grep -hE '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

.env:
	cp .env.example .env

# ------------------------------------------------------------------ stack
up: .env ## Build and start everything (Temporal, Java + Python apps, mock data, Postgres) and promote worker versions
	$(COMPOSE) up -d --build --wait
	$(COMPOSE) --progress quiet run --rm deploy-setup
	$(COMPOSE) build demo
	@$(MAKE) --no-print-directory urls

down: ## Stop everything (data kept)
	$(COMPOSE) --profile elk --profile tools --profile setup down

clean: ## Stop everything and delete all data (volumes)
	$(COMPOSE) --profile elk --profile tools --profile setup down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) --profile elk ps -a

logs: ## Follow logs (S=service, e.g. make logs S=stp-worker)
	$(COMPOSE) logs -f --tail 100 $(S)

urls: ## Print the UIs
	@source .env; echo; \
	 echo "  Temporal UI     http://localhost:$${TEMPORAL_UI_HOST_PORT:-8233}"; \
	 echo "  Mock data       http://localhost:8090/demo/claims"; \
	 echo "  Kibana (elk)    http://localhost:5601"; echo

pilot: ## Pilot scope: Lifecycle -> STP only (TSA/IS disabled for new claims)
	TSA_ENABLED=false IS_ENABLED=false $(COMPOSE) up -d bridge

full: ## Back to all tracks (TSA + IS enabled)
	$(COMPOSE) up -d bridge

# ------------------------------------------------------------------ our answers to the 7 questions, live
q1: ## Q1 parent + children: new child per visit, stale results rejected by the system of record
	$(CLI) q1
q2: ## Q2 close / reopen: reopen window, same Workflow ID new run, stage numbering continues (~2 min)
	$(CLI) q2
q3: ## Q3 long-term history: business projection linked to the exact executions
	$(CLI) q3
q4: ## Q4 events: dedupe, buffering, dead letters; approvals as validated idempotent Updates
	$(CLI) q4
q5: ## Q5 Java/Python contracts: schema-generated models, forward compatibility
	$(CLI) q5
q6: ## Q6 changes mid-flow: re-read, material-change filter, cancel (not payments), backstop poll
	$(CLI) q6
q7: ## Q7 YAML flows: narrow DSL, pinned flow per claim, authority enforced in code
	$(CLI) q7
questions: q1 q2 q3 q4 q5 q6 q7 ## All seven, one after another (~8 min)

# ------------------------------------------------------------------ demo / operator commands
demo: ## Run all 6 scripted claims end to end and check every outcome (~2 min)
	$(CLI) demo

seed: ## File a claim from a fixture (FIXTURE=CLM-1002 [ID=...]); prints the claim id
	$(CLI) seed $(FIXTURE) --id $(or $(ID),$(FIXTURE)-$(SUFFIX)) | grep -m1 '"claimId"'

state: ## Lifecycle state + transitions (ID=...)
	$(call need,ID,CLM-1002-153012)
	$(CLI) state $(ID)

progress: ## Active STP child progress (ID=...)
	$(call need,ID,CLM-1002-153012)
	$(CLI) progress $(ID)

patch: ## Change claim data in ClaimsPro (ID=... FIELDS="injuryReported=true")
	$(call need,ID,CLM-1002-153012)
	$(call need,FIELDS,injuryReported=true)
	$(CLI) patch $(ID) $(FIELDS)

decide: ## Adjuster decision via Update (ID=... ACTION=CLOSE|RETURN_TO_STP|RETRIAGE)
	$(call need,ID,CLM-1002-153012)
	$(call need,ACTION,CLOSE)
	$(CLI) decide $(ID) $(ACTION)

ledger: ## Payments + idempotent replays (ID=...)
	$(call need,ID,CLM-1001-153012)
	$(CLI) ledger $(ID)

reopen: ## Reopen a closed claim (ID=...)
	$(call need,ID,CLM-1001-153012)
	$(CLI) reopen $(ID)

cancel-payment: ## Request payment cancellation (ID=...)
	$(call need,ID,CLM-1001-153012)
	$(CLI) cancel-payment $(ID)

history: ## Business history from the projection table (ID=...)
	$(call need,ID,CLM-1002-153012)
	$(COMPOSE) exec -T postgres psql -U claims -c "select stage_version v, track, step, status, detail from claim_projection where claim_id='$(ID)' order by occurred_at, stage_version"

actions: ## Temporal Cloud Actions used by claims (ID="id1 id2 ...")
	$(call need,ID,CLM-1001-153012)
	$(RUN) python -m demo.actions $(ID)

loadtest: ## CAT surge (CLAIMS=300 RATE=30)
	$(RUN) python -m demo.loadtest --claims $(CLAIMS) --rate $(RATE)

# ------------------------------------------------------------------ tests (inside Docker builds)
test: test-python test-java ## Run all tests

test-python: ## Python tests (17)
	docker build -f python/Dockerfile --target test --no-cache-filter test --progress=plain . 2>&1 | grep -E 'passed|failed|error|ERROR' || true
	@docker build -q -f python/Dockerfile --target test . >/dev/null && echo "python tests OK"

test-java: ## Java tests (36, first run downloads Maven deps)
	docker build -f java/Dockerfile --target test --no-cache-filter test --progress=plain . 2>&1 | grep -E 'Tests run:|FAIL|BUILD|ERROR' || true
	@docker build -q -f java/Dockerfile --target test . >/dev/null && echo "java tests OK"

# ------------------------------------------------------------------ ELK
elk-up: ## Start Elasticsearch, Kibana, Filebeat, Metricbeat (needs ~3 GB more RAM)
	$(COMPOSE) --profile elk up -d
	@echo "Kibana takes 3-5 min: http://localhost:5601 -> Discover -> 'Claims logs (Java + Python)'"

elk-down: ## Stop ELK (frees memory for demo / load test)
	$(COMPOSE) --profile elk stop kibana-setup kibana filebeat metricbeat elasticsearch
