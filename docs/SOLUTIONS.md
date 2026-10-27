# Our solution to the seven questions

For each question in *Claims Reimagination – Temporal*: what was proposed, what we recommend,
**what we changed or added compared with their proposal**, where it is implemented, and how to show
it. The full reasoning with alternatives, pros and cons is in [DECISIONS.md](DECISIONS.md).

Paths are relative to `java/src/main/java/com/example/claims/` (Java) and `python/claims/` (Python).

---

## Q1. Parent lifecycle pattern

**Proposed:** one Java parent per claim owning track transitions; TSA and IS (Python) and
STP (Java) children; stage versions and lease tokens to reject stale results.

**Our solution:** keep the parent, but keep it **thin**: it knows only track names, task queues, the
`TrackRequest`/`TrackResult` contract and a **pure routing table**. Children return an *outcome*,
never the next track. **Every track visit is a new child execution** whose ID carries the stage
version. The stage version is a lease inside Temporal **and a fence at the system of record**:
ClaimsPro rejects writes carrying an older stage.

**What we changed or added**
- The fence moves from "check in the workflow" to "**enforced by ClaimsPro**". A workflow-side check
  cannot stop an activity that is already running against an external system.
- Routing is a pure, unit-tested function (`Routing`), so the parent cannot grow track logic.
- The parent starts children **by name** (untyped stub), so Java has no compile-time link to Python.
- A **re-triage cap** escalates endless TSA ↔ STP loops.

**Code:** `temporal/workflows/lifecycle/ClaimLifecycleWorkflowImpl.java`, `Routing.java`;
`temporal/activities/StepContext.java` (stage + idempotency tokens).
**Show:** `make q1`. **Tests:** `RoutingTest`, `retriageLoopIsBoundedThenEscalates`.

## Q2. Closing and reopening

**Proposed:** an open question (complete at close, or stay open for a reopen window? same
Workflow ID or new?).

**Our solution:** stay open in CLOSED for a **bounded reopen window**, then complete. Reopen inside
the window is handled in the same run. Reopen after completion starts a **new run with the same
Workflow ID** (`claim:{claimId}`) via Signal-With-Start, and the new run **continues the stage
numbering from ClaimsPro**. Continue-As-New at track boundaries when Temporal suggests it or above
10,000 events, **and also while a claim waits in SIU/ESCALATION** for a long time.

**What we changed or added**
- Only FNOL and REOPEN may start a workflow. Any other event for a finished claim is dead-lettered,
  so a stray update cannot resurrect a closed claim.
- **Bug class we found and fixed:** a new run that restarts at stage 1 is rejected by ClaimsPro's
  fence and **reuses the old run's idempotency keys**, so the reopened claim's payment would come
  back as a "replay" of the old payment. The run now seeds its stage from ClaimsPro.
- **Race we found and fixed:** a decision accepted just before Continue-As-New was lost, because it
  is not part of the checkpoint. Continue-As-New now waits until the decision is applied.

**Code:** `ClaimLifecycleWorkflowImpl` (`awaitReopenWindow`, `continueAsNewIfDue`, `seed-stage-version`),
`temporal/client/EventBridge.java`.
**Show:** `make q2`. **Tests:** `reopenAfterCompletionContinuesStageVersionsInANewRun`,
`longWaitInSiuContinuesAsNewAndStillAcceptsTheDecision`, `continueAsNewCarriesStateForward`.

## Q3. Long-term business data

**Proposed:** an application-owned projection table written by idempotent activities keyed
`claimId:stageVersion:step`; open question whether workflows wait for each write.

**Our solution:** yes to the projection as the **business record** (years, SQL, open and closed
claims together). Temporal history and archival are the **developer record**. Every row stores
`workflow_id` and `run_id`, which joins the two layers the design asked for. Writes are **async and
awaited at track boundaries** (STP) or before completion / Continue-As-New (parent): no step blocks
on the database, and no row can be lost.

**What we changed or added**
- The explicit **two-layer link** (`workflow_id` + `run_id` on each business row).
- Writes from a step cancelled by re-triage still land (detached cancellation scope).
- Reconciliation from ClaimsPro, not from Temporal history, if a row is ever missing.
- Pass IDs to activities and re-read data, but **record the values a decision used** (the paid
  amount) so the decision stays explainable.

**Code:** `temporal/activities/ProjectionActivitiesImpl.java`, `db/JdbcProjectionStore.java`,
`db/postgres/init.sql`, `StpWorkflowImpl.record()/finish()`.
**Show:** `make q3`, `make history ID=…`.

## Q4. Event bridge and signal routing

**Proposed:** bridge normalises allowlisted ClaimsPro events and signals the parent; the
parent forwards to the active child. Open: awkward timing, 100–150 events, Signal vs Update for approvals.

**Our solution:** keep **bridge → parent → active child**. Signal per event, **deduplicated by
eventId** in parent and child. Events with no consumer (between tracks, irrelevant to the active
track, after a child finished) are **buffered, newest per type**, and handed to the next child, which
**re-reads ClaimsPro anyway**. Events for a finished claim are **dead-lettered**. Adjuster approvals
are an **Update on the parent**: a validator rejects wrong claim / wrong track / unknown action
before anything reaches history, and a retried `decisionId` returns `duplicate=true`.

**What we changed or added**
- Buffering with coalescing, so the buffer can never grow unbounded.
- The PII allowlist in the bridge (only `changedFields`, `documentType` cross into Temporal).
- **Race we found and fixed:** an Update accepted while the parent was still switching tracks was
  silently dropped. Fixed and versioned with `getVersion`.
- Bridge-side debouncing kept as an *option* for CAT volume, not the default.

**Code:** `temporal/client/EventBridge.java`, `ClaimLifecycleWorkflowImpl` (`claimEvent`, `drainInbox`,
`validateAdjusterDecision`, `submitAdjusterDecision`).
**Show:** `make q4`. **Tests:** `eventsAreDeduplicatedAndForwardedToTheActiveChild`,
`adjusterDecisionIsValidatedAndIdempotent`, `decisionAcceptedWhileTheTransitionIsStillRunningIsNotLost`.

## Q5. Java and Python contracts

**Proposed:** canonical JSON with generated models, additive-only evolution, explicit enum
fallback, golden tests in both languages.

**Our solution:** exactly that, made concrete: **one JSON Schema** generates Java (jsonschema2pojo)
and Python (pydantic) models; `schemaVersion` on every message; readers ignore unknown fields;
unknown enum values become `UNKNOWN`, and the parent routes an unknown child outcome to MANUAL
instead of failing; the **same golden files** are round-tripped by both test suites.

**What we changed or added**
- A forward-compatibility golden sample (newer writer, unknown field + unknown enum).
- Agreed rules for the differences that bite: null vs absent, Python keywords (`from`), money as
  integer cents, ISO timestamps, shared non-retryable error names, untyped child stubs.

**Code:** `contracts/schema/claims-contracts.v1.json`, `contracts/golden/`,
`temporal/client/ContractJson.java`, `python/claims/contracts/base.py`.
**Show:** `make q5`. **Tests:** `ContractGoldenTest`, `test_contracts.py`.

## Q6. Continuous monitoring

**Proposed:** small signal → dedupe → re-read ClaimsPro → act after revalidation → timer or
poll as backstop. Open: in-flight activities, race testing.

**Our solution:** the proposed pattern, plus a **material-change filter** (injury, LOB, loss type,
policy, coverage, loss +50%), so cosmetic edits cause no churn. On a material change mid-stage,
**cancel the in-flight work and re-triage, except in payment stages**, which finish first; the
stage fence rejects anything late. Backstops: re-read at each stage boundary, poll while waiting
for external events.

**What we changed or added**
- The "never interrupt a payment" rule, enforced in compiled code (not in YAML).
- A testing approach that found two real races: time-skipping server, **gated activity stubs** to
  inject a message mid-activity, and **replay of captured real histories** in CI.
- AI calls capped at 5 attempts: an AI outage routes the claim to a human instead of retrying a paid
  model forever.

**Code:** `temporal/workflows/stp/StpWorkflowImpl.java`, `MaterialChange.java`,
`temporal/workflows/stp/flow/FlowInvariants.java`, `python/claims/temporal/workflows/support.py` (`AI_RETRY`).
**Show:** `make q6`. **Tests:** `StpWorkflowTest`, `ReplayTest`, AI retry test.

## Q7. YAML/JSON flow definitions

**Proposed:** a compiled workflow interpreting a validated, immutable definition; pinned
version and hash; registered operations only; payment authority and invariants in code.

**Our solution:** yes, as a **narrow DSL**. YAML orders registered steps, marks steps as parallel or
optional, and sets limits. Nothing else is expressible. The flow is resolved **in an activity** at
track start, so it is recorded in history and **pinned** (`id@version` + content hash) for that
claim. The authority ceiling and the payment rule stay in Java. Unknown steps fail at worker start.

**What we changed or added**
- Most-specific-match selection by LOB / state / channel (the "variations" shown on the kickoff call).
- Interpreter changes guarded with `getVersion` and checked by replaying captured histories of every flow.
- A clear line on when a DSL is worth it, and a recommendation not to grow it into conditions or loops.

**Code:** `config/flows/*.yaml`, `temporal/workflows/stp/flow/` (`FlowRegistry`, `FlowInvariants`).
**Show:** `make q7`. **Tests:** `FlowRegistryTest`, `ReplayTest`.

## Deployment questions

| Question | Our solution | Where |
|---|---|---|
| AUTO_UPGRADE parent, PINNED children? | **Implemented**: Worker Deployment Versioning per track; parent AUTO_UPGRADE (with `getVersion` + replay tests), children PINNED; versions promoted by `deploy/deploy-setup.sh` | `temporal/workers/Workers.java`, `python/claims/temporal/workers/main.py` |
| Isolating STP load | One task queue and one deployment per track | `docker-compose.yml` |
| claimId as Search Attribute | Yes, if the claim number holds no PII; set on every execution | `temporal/SearchAttrs.java` |
| Encryption with ID-only payloads | Recommended before production (one shared Java/Python codec); not in the demo | DECISIONS.md |
| Event bridge in production | The HTTP bridge here is a demo stand-in: production consumes the change feed from a durable log and commits only after the Signal succeeds | `EventBridge.java` (class comment) |
