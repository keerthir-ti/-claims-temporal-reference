# Design decisions — Claims Reimagination on Temporal

This document answers the questions in *Claims Reimagination – Temporal* (and the follow-ups from the
kickoff call) one by one. For each one it gives the options with their pros and cons, the approach
this repository takes, and the file where you can see it running. Where a number depends on
Temporal Cloud's current limits or pricing, it is marked **[confirm with Temporal]** so it can go on
the agenda for the next session.

Legend: **✅ Recommended** = implemented in this repo · **⚖ Alternative** = valid, with trade-offs ·
**✗ Avoid** = known anti-pattern.

## Summary

| # | Question | Recommendation | See |
|---|---|---|---|
| 1 | Parent lifecycle pattern | ✅ One parent per claim that **only routes**. A **new child execution per track visit**. **stageVersion** works as both a lease token and a fencing token. | `workflows/lifecycle/ClaimLifecycleWorkflowImpl`, `workflows/lifecycle/Routing` |
| 2 | Closure & reopen | ✅ Stay open in CLOSED for a **bounded reopen window**, then complete. A later reopen starts a **new run with the same Workflow ID** via Signal-With-Start. Continue-As-New on `isContinueAsNewSuggested()` or about 10k events. | `ClaimLifecycleWorkflowImpl.awaitReopenWindow`, `client/EventBridge` |
| 3 | Long-term business data | ✅ App-owned **projection table** written by an idempotent activity (`claimId:stageVersion:step`). Workflows wait for these writes **only at track boundaries**. | `activities/ProjectionActivitiesImpl`, `db/`, `db/postgres/init.sql` |
| 4 | Event bridge & routing | ✅ bridge → parent → active child. Signal per event with **eventId dedupe**. Events with no consumer are **buffered and coalesced by type**. Children **re-read ClaimsPro** at start. Human decisions use an **Update on the parent**. | `client/EventBridge`, `ClaimLifecycleWorkflowImpl.drainInbox` |
| 5 | Java ↔ Python contracts | ✅ **JSON Schema** → generated Java + Python models. Additive-only changes, `schemaVersion` on every payload, an `UNKNOWN` fallback for enums, and **golden files tested in both languages**. | `contracts/`, `ContractJson.java`, `contracts/base.py` |
| 6 | Continuous monitoring | ✅ Your 5-step pattern, as-is, plus a **material-change filter**. Cancel in-flight work only in stages that don't touch money. Race tests run on the **time-skipping server** and against **replayed real histories**. | `workflows/stp/StpWorkflowImpl`, `workflows/stp/MaterialChange`, tests |
| 7 | YAML/JSON flows | ✅ Sound, **if the scope stays narrow**: registered steps only, money rules compiled in, the flow pinned by version + hash at start, and interpreter changes gated by `getVersion` and checked by replay tests. | `workflows/stp/flow/*`, `config/flows/*.yaml`, `ReplayTest` |

---

## 1. Parent lifecycle pattern

**Question.** Is one parent coordinating TSA/IS/STP children recommended? Where should the boundary
sit? Should each track visit get a new child, or should one child be reused? Are stage versions and
lease tokens a good way to reject stale results?

### Options

| Option | Pros | Cons |
|---|---|---|
| ✅ **Parent routes, children execute** (your design) | One place owns the lifecycle state, so "where is claim X?" is a single Query. Tracks deploy independently on their own task queues. Re-triage loops and limits can be enforced centrally. | The parent has to stay thin, or it turns into a monolith. Event forwarding adds a small Actions cost (see §4). |
| ⚖ Children hand off to each other (STP starts TSA directly) | Fewer hops | Nobody owns the lifecycle, re-triage loops are unbounded, and every track has to know every other track. The kickoff confirmed this is **not** wanted. |
| ⚖ One big workflow per claim, no children | Simplest code | Mixes languages badly (one workflow = one SDK), history grows with everything, and a single deploy covers all tracks. |
| ⚖ Entity workflow per claim + separate track workflows, linked only by Signals | Very loose coupling | You lose parent/child semantics (cancellation propagation, result delivery) and have to rebuild them with Signals. |

### Boundary rule used here

The parent knows **only**: the track names, the child workflow type name and task queue for each
track, the `TrackRequest`/`TrackResult` contract, and a pure routing table (`Routing.java`). It never
imports track code. It starts children through an **untyped stub by name**
(`Workflow.newUntypedChildWorkflowStub(Names.workflowTypeFor(track))`), so the Python children have
no compile-time link to the Java parent.

- Children return an **outcome** (`ROUTE`, `COMPLETED`, `RETRIAGE_REQUESTED`, `ADJUSTER_HANDOFF`).
  The parent decides the next track. Even TSA's "route to STP" is only a recommendation, and the
  parent validates it (`Routing.afterTrack`): for example, TSA may not route to CLOSED, and IS is
  rejected when it is disabled.
- Track business logic stays in the track. Adding a step to STP never touches the parent.
- The test for "has the parent become a monolith?" is to ask whether a change inside a track ever
  requires a parent deploy. If it does, the boundary has leaked.

### New child per visit vs reuse

✅ **A new child execution for every track visit**, with ID `claim:{claimId}:{track}:{stageVersion}`.

| | New child per visit ✅ | Reuse one long-lived child per track |
|---|---|---|
| History | Each visit has its own small, readable history | Grows across visits, so it needs its own Continue-As-New |
| Stale work | A finished child cannot act again. Its results carry an old `stageVersion`. | It must be told "you are no longer active" and reset itself |
| Versioning | Each visit starts on the current worker version | Stays on whatever version it started on |
| Debugging | The Workflow ID tells you which visit it was | You have to work out which part of the history belongs to which visit |

### Stage version as lease and fencing token

Every transition increments `stageVersion` (`transition()`), and the new value travels in three
places:

1. In the **child's input and Workflow ID**. The parent ignores a result whose `stageVersion`
   doesn't match the current one (the defensive check in `runChildTrack`).
2. To **ClaimsPro**. On every transition the parent calls `beginStage(claimId, stageVersion)`, and
   every write activity sends `X-Stage-Version`. ClaimsPro rejects writes from an older stage with
   409, which the client maps to a non-retryable `StaleStage` error. This is the part that actually
   stops a **late activity completion** from an abandoned stage: a retry that wakes up after
   re-triage cannot pay or close the claim.
3. In the **projection key** (`claimId:stageVersion:step`).

> Pitfall: a check inside the workflow alone is not enough. An activity that was already
> dispatched still runs against the external system. **Fencing has to happen at the system of
> record** (or in a guard the activity checks before any side effect). If ClaimsPro cannot add a
> stage check, put a thin "write gateway" in front of it that does.

---

## 2. Business closure and reopening

### Options

| Option | Pros | Cons |
|---|---|---|
| Complete immediately at CLOSED | Smallest number of open executions. Cheapest. | Every reopen needs a new run. Events that arrive just after closing (late docs, payment reversals) find no workflow. |
| ✅ **Stay open in CLOSED for a bounded window, then complete** | Late events in the window are handled in-workflow (reopen → re-triage). After that, the workflow costs nothing. | One timer per closed claim during the window (cheap). The window length is a business decision. |
| ✗ Stay open forever | — | Unbounded open executions, history keeps growing, and every worker deploy has to stay compatible with years-old code paths. |

Implemented: `awaitReopenWindow()` (`REOPEN_WINDOW_SECONDS`, 120s in the demo; production would
use something like 30–90 days as the business decides). Reopen inside the window → back to TSA in the
**same run**. When the window ends, the workflow records `LIFECYCLE_COMPLETED` in the projection
and completes.

### Reopen after completion: same Workflow ID or new?

✅ **Same Workflow ID (`claim:{claimId}`), new run**, started by the bridge with
**Signal-With-Start** and `WorkflowIdReusePolicy.ALLOW_DUPLICATE`. One ID per claim keeps lookups,
Search Attribute queries and ELK correlation trivial. The runs are separated by Run ID, and the
projection (keyed by `stageVersion`) continues the business timeline.

The reopened run starts with `reopened=true` → TSA. It **re-reads ClaimsPro**, so it doesn't
depend on the old run's history, which may already be gone after retention.

### ID reuse, retention, Signal-With-Start pitfalls

- **Only FNOL and REOPEN may Signal-With-Start.** Every other event is a plain Signal. If no run is
  open, the event goes to the dead-letter log (`DEAD_LETTER` in ELK) instead of resurrecting a
  closed claim (`EventBridge.accept`). This is the most common Signal-With-Start mistake.
- A duplicate FNOL while the run is open just becomes a Signal, which the parent dedupes by
  eventId. Use a conflict policy of *use existing*, never *terminate existing*.
- Retention (Cloud: 1–90 days **[confirm with Temporal]**) only affects **closed** runs. Do not use
  Temporal as the long-term record (see §3).
- **`stageVersion` must keep increasing across runs.** A new run that restarts at 1 is rejected
  by ClaimsPro's fence (stage 1 < stage N), and worse, it **reuses the previous run's idempotency
  keys**, so the reopened claim's payment would come back as a "replay" of the old payment instead
  of a new one. A new run therefore seeds `stageVersion` from ClaimsPro's current stage
  (`LifecycleActivities.currentStageVersion`). This was found and fixed while building the repo,
  and it is covered by `reopenAfterCompletionContinuesStageVersionsInANewRun`. It was verified live:
  a reopened claim ran stages 4–6 and produced a second, distinct payment.

### When to Continue-As-New

✅ `Workflow.getInfo().isContinueAsNewSuggested()` **or** `historyLength > CAN_EVENT_THRESHOLD`
(default 10,000), checked at **track boundaries**, never in the middle of a child. State carried
over is bounded: track, stageVersion, retriage count, the last 50 transitions, the last 500 event
and decision IDs, and the buffered events (`LifecycleCheckpoint`).

Reference limits **[confirm with Temporal]**: warnings at about 10,240 events / 10 MB, with the
execution terminated at 51,200 events / 50 MB. With your expected volume (100–150 events over
weeks plus about 15 events per track visit), a claim reaches the threshold only through
pathological re-triage loops, and those are already capped by `MAX_RETRIAGE` → ESCALATION. The
`continueAsNewCarriesStateForward` test forces CAN at 40 events to prove it works.

---

## 3. Long-term business data

### Who owns what

| Store | Purpose | Lifetime | Audience |
|---|---|---|---|
| **Temporal Event History** | Durable execution + replay. The exact order of signals, activities and timers. | Retention period after close (days) | Developers / on-call |
| **Temporal Archival** (S3) | Cold copy of closed histories for forensic replay ("why did we pay 6 months ago?") | Years, in S3 | Developers. Not for business queries. |
| **Cloud Audit Logs** | Who changed the namespace, users, keys (control plane) | Per Cloud settings | Security / compliance |
| **Operational logs (ELK)** | Debugging, correlation by WorkflowId/claim.id, alerting | Weeks (ILM) | Developers / SRE |
| ✅ **Business projection (Postgres)** | The business timeline: transitions, decisions, payments, *why* | Years, queryable with SQL | Business, adjusters, reporting |

This is the "two layers" from the meeting notes. The **business layer** is the projection. The
**developer layer** is ELK plus Temporal history or archive. Both carry `workflow_id` + `run_id`,
so a business row ("payment issued, stage 4") links straight to the exact execution. That is the
"two into one" traceability the team asked for, and it doesn't require merging the stores.

### Projection vs querying S3 (archival) directly

| | ✅ Projection table | S3 archive + Athena |
|---|---|---|
| "Adjuster X completed 30 claims, 2 still open" | One SQL query over active **and** closed claims (`claim_current` view) | Closed claims only. Active ones are not archived yet, so you'd need to merge two sources. |
| Schema | Business columns you design | Raw Temporal event JSON, with payloads encoded or encrypted |
| Cost / speed | Indexed, milliseconds | Scan cost, seconds to minutes |
| Use for | Reporting, the adjuster UI, audit of decisions | Deep forensic replay of a specific run |

✅ Use **both**: the projection for every business question, archival only for forensic replay.

### Should workflow progress wait for the projection write?

| Option | Pros | Cons |
|---|---|---|
| Block on every write | Strictly consistent | Adds latency to every step. A projection DB outage stops claims. |
| Fire-and-forget | Fast | A write can be lost if the workflow completes first |
| ✅ **Async writes, awaited at track boundaries** | Steps don't block. Writes are guaranteed before the track result is returned. A DB outage only delays the end of a track (the activity retries). | Rows land seconds late (eventually consistent) |

Implemented in `StpWorkflowImpl.record()` (async, inside a **detached cancellation scope**, so a step
cancelled by re-triage still gets its row) and in `finish()` (`Promise.allOf(projectionWrites).get()`).
Writes are idempotent `INSERT … ON CONFLICT DO NOTHING` on `projection_key`, so activity retries
are safe.

### Recovery if a row is missing after history has expired

1. **Prevention:** because tracks wait for their writes, a missing row means a failed activity.
   Alert on `recordProjection` failures (ELK saved search *Activity retries*).
2. **Reconciliation job** (nightly): compare ClaimsPro's state with `claim_current`. Where they
   differ, write a `RECONCILED` row sourced from ClaimsPro, the system of record. This doesn't need
   Temporal history.
3. **Forensic:** if archival is on, replay the archived history to regenerate the rows.

> Pitfall seen in this repo: `occurred_at` is workflow time, and steps recorded in the **same
> workflow task share a timestamp** (CLOSE_CLAIM and STP_COMPLETED tie in the sample). If strict
> ordering matters for reporting, add a per-execution sequence number to `ProjectionRecord`.

### Payload: references vs data (meeting notes)

✅ **Pass references, load in the activity.** Workflow inputs carry `claimId` + `stageVersion`, and
each activity fetches what it needs from ClaimsPro at execution time. If an activity is retried a
day later, it runs on **current** data, which is what the team wanted. Payloads stay small and free
of PII.

The exception is a **decision** that must be explainable later, such as the estimate amount that
was paid. Record that value in the workflow (activity result) and in the projection. Otherwise
"why did we pay $2,450?" can't be answered once ClaimsPro changes.

### Idempotent side effects ("ran once, created a payment, failed, reran")

✅ Every write sends `Idempotency-Key = claimId:stageVersion:step` (`StepContext.idempotencyKey`).
ClaimsPro returns the original result for a repeated key. CLM-1001 demonstrates it: the mock
**commits the payment and then returns 503**, Temporal retries, and the retry gets the existing
payment (`"replayed": true`). The ledger shows exactly one payment.

If ClaimsPro can't support idempotency keys: the activity first **looks up** a payment by an
external reference (`claimId:stageVersion`) and creates one only if none exists. That needs a
unique constraint on that reference in ClaimsPro, or the lookup has a race.

---

## 4. Event bridge and signal routing

### Bridge → parent → child, or bridge → child?

| | ✅ Bridge → parent → child | Bridge → child directly |
|---|---|---|
| Bridge complexity | Stateless: allowlist, normalize, derive `claim:{id}`, Signal | Must know which track and stage is active (a query or a lookup table), and that goes stale during transitions |
| Transitions | The parent buffers events while no child runs | Events sent mid-transition hit a finished child and are lost |
| Cost | About **+1 Action per forwarded event** (the parent's signal to the child) **[confirm with Temporal]** | 1 Action per event |
| Coupling | The bridge knows one Workflow ID format | The bridge knows every track's ID format |

✅ Keep the bridge dumb. The extra Action is the price of correctness during transitions.

### Events at awkward moments

| When | Handling here |
|---|---|
| Before the first child starts | Inbox → **buffered**, then passed in `TrackRequest.bufferedEvents` |
| During a track transition | Same as above. The parent waits for `child.getExecution()` before it forwards anything. |
| Not relevant to the active track | Buffered for the next track (the `RELEVANT` map per track) |
| After the child completes | Events still in the inbox are buffered (`activeChild = null` before draining) |
| After parent completion | Bridge → `WorkflowNotFound` → **dead letter** (reconcile from ClaimsPro); FNOL/REOPEN start a new run |
| Duplicates | Deduped by `eventId` in both parent and child (bounded sets carried across CAN) |

✅ **Buffer and coalesce by event type** (a newer CLAIM_UPDATED replaces an older one), **and** have
children re-read ClaimsPro when they start. The buffer is only a hint. The re-read is the truth, so
a lost buffered event costs nothing.

### 100–150 low-information events per claim

| Option | Pros | Cons |
|---|---|---|
| ✅ **Signal per event + eventId dedupe** (implemented) | Simple, nothing lost, ordered per claim | 150 × (1 Signal + forwarding) Actions per claim; history grows by about 2 events per signal |
| ⚖ Coalesce in the bridge (debounce CLAIM_UPDATED per claim for 30–60 s) | Cuts Signals by 5–10× during bursts (CAT) | The bridge needs a small state store. Adds latency. |
| ⚖ Filter in the bridge by changed fields | Drops irrelevant updates before Temporal | The bridge starts to know business rules |

Recommendation: start with signal-per-event (correct, simple). Add **bridge-side debouncing for
`claim.updated` only** if Actions or CAT surges call for it. Debouncing is safe because consumers
re-read ClaimsPro anyway. 150 signals is far below the per-execution signal limit (10,000
**[confirm with Temporal]**).

### Human approval: Update or Signal? Parent or child?

✅ **Update, on the parent** (`submitAdjusterDecision`).

- An Update has a **validator**. A decision for the wrong claim, the wrong track, an `UNKNOWN`
  action, or a duplicate is rejected before it is written to history, and the adjuster UI gets an
  immediate, synchronous error. A Signal is fire-and-forget: the UI can't know whether it was
  applied.
- Idempotency: the client generates a `decisionId`. A retried Update with the same ID returns
  `duplicate=true` instead of applying twice (CLM-1004).
- On the parent: human tracks (MANUAL/SIU/ESCALATION) are parent states, so there is no child to
  target. For an approval *inside* a child (for example, an STP payment approval step), send the
  Update to the parent and let the parent forward it, so the UI keeps one stable target
  (`claim:{id}`).

> **Race found and fixed while building this** (`decisionAcceptedWhileTheTransitionIsStillRunningIsNotLost`):
> the parent switches its track to SIU and then runs the `beginStage` and projection activities. An
> Update that arrives in that window passes validation, because the track is already SIU. The old
> code then reset `pendingDecision` when it entered `awaitAdjuster()`, which **dropped a decision
> the adjuster had already seen acknowledged**. The fix clears the decision on transition instead of
> on wait. It is guarded by `Workflow.getVersion("keep-early-adjuster-decision")` so executions
> already running keep replaying. This is exactly the class of bug §6's testing approach is for.

---

## 5. Java and Python contracts

### JSON Schema or Protocol Buffers?

| | ✅ JSON Schema + generated models | Protocol Buffers |
|---|---|---|
| Temporal UI / CLI | Payloads readable as JSON | Binary by default (JSON-proto mode exists, but SDK support differs) |
| Code generation | `jsonschema2pojo` (Java), `datamodel-code-generator` (Python), run at build time | `protoc`: very mature, one toolchain |
| Evolution | Additive rules you enforce yourself (golden tests) | Field numbers make additive evolution natural |
| Enums | Strings. An unknown value needs an explicit fallback (done). | Unknown values are preserved as ints |
| Existing Java models (STP reuses backend contracts) | Easy to align with Jackson models already in use | Needs a mapping layer |
| Default converter | Works with the Java Jackson converter and the Python pydantic converter | Needs the proto converter in both languages |

✅ JSON Schema is the right call **for this team**: it matches the existing Java models, it's
readable in the Temporal UI and ELK, and it's what was proposed. Choose protobuf instead if
contracts will be shared with many more languages or services, or if payload size matters.

### Rules implemented

- **One schema** (`contracts/schema/claims-contracts.v1.json`) → Java classes at Maven
  `generate-sources`, Python pydantic models. Hand-editing generated code is not allowed.
- **`schemaVersion` on every payload.** It is not used for dispatch today. It exists so a future
  breaking change can be detected and handled explicitly.
- **Tolerant reader:** unknown fields are ignored in both languages (`FAIL_ON_UNKNOWN_PROPERTIES=false`
  in Jackson, `extra="ignore"` in pydantic). No `additionalProperties: false` in the schema.
- **Enum fallback:** unknown enum strings become `UNKNOWN` instead of failing the workflow task
  (`ContractJson.java`, `contracts/base.py`). Routing sends `UNKNOWN` outcomes to MANUAL.
- **Additive-only:** new fields are optional, with defaults. Never rename, never change a type,
  never remove a field while histories that contain it are still inside retention.
- **Golden payloads** (`contracts/golden/*.json`, including `forward-compat/` with extra fields and
  unknown enums) are deserialized and re-serialized by **both** `ContractGoldenTest` (Java) and
  `test_contracts.py` (Python).
- **Stable names** (`Names.java` / `names.py`): workflow types, task queues, signal/update/query
  names. These are the real cross-language contract and must never change once used.

### Java/Python differences to settle early (all hit while building this)

| Area | Difference | What this repo does |
|---|---|---|
| Null vs absent | Jackson writes `null` fields by default; pydantic may omit them | Generated Java classes use `@JsonInclude(NON_NULL)`; Python treats null and absent the same |
| Reserved words | `Transition.from` is a Python keyword | Generated as `from_` with alias `from`; the wire name stays `from` |
| Numbers | Java `long` vs Python unbounded `int`; JSON floats | Money is **integer cents** everywhere; no floats in contracts |
| Timestamps | `Instant` vs `datetime` | ISO-8601 strings in contracts |
| Failures | Java `ApplicationFailure` type strings vs Python `ApplicationError` | Shared non-retryable type names: `StaleStage`, `ClaimNotFound`, `BusinessRuleViolation` |
| Search Attributes | Typed SA APIs differ (`SearchAttributeKey` vs `SearchAttributeKey.for_keyword`) | Same names, registered once per namespace |
| Child stubs | Java parent cannot use a typed stub for a Python child | Untyped stub by name + contract class |
| Worker Deployment Versioning | Both SDKs support it, but the API surfaces and maturity differ | See README (Moving to production); settle the version-id scheme before go-live |

---

## 6. Continuous monitoring

**Your 5-step pattern is the recommended one.** This repo implements it in STP:

1. Small canonical Signal (`ClaimEvent`: id, type, version, allowlisted attributes only).
2. Dedupe by `eventId`.
3. Re-read ClaimsPro (`fetchClaim`). Skip it if `version` didn't increase.
4. Change course only if `MaterialChange.detect(old, new)` says so. A change in line of business
   or loss type, a newly reported injury, a policy that became inactive, coverage leaving
   CONFIRMED, or estimated loss up by more than 50% counts as material. A cosmetic update does
   nothing.
5. Backstops: a re-read at **every stage boundary** if the snapshot is older than
   `REVALIDATE_INTERVAL_SECONDS`, plus a **polling timer** while waiting for external events
   (CLM-1006 drops the estimate event, and the poll finds it).

### Inside the workflow, or a separate monitoring workflow?

| Keep inside the track workflow ✅ | Move to a separate child / entity workflow |
|---|---|
| The reaction changes *this* track's flow (cancel the stage, re-triage) | Monitoring outlives tracks (for example, watching a policy for months) |
| Low event rate (≤ a few hundred per track visit) | High-rate feeds that would bloat the track's history |
| | Several workflows must react to the same condition (one monitor, many subscribers) |

### Condition change vs an in-flight activity

✅ **It depends on the stage, and the rule lives in compiled code** (`FlowInvariants.interruptible`):

- **Interruptible stage** (contact, reserve, inspection, estimate wait): **cancel** the cancellation
  scope and return `RETRIAGE_REQUESTED`. Cancelled steps still get their projection row.
- **Money stage** (`ISSUE_PAYMENT`, `FINALIZE_PAYMENT`): **never cancelled**. The stage finishes,
  and re-triage happens at the next boundary. A payment cancel request is an explicit branch
  (`cancelPayment`) between "create" and "finalize", as the meeting notes describe.
- **Always**, as the safety net: stage-version fencing at ClaimsPro (§1) rejects any late
  completion from a stage that was abandoned.

### Testing races

| Technique | What it catches | Here |
|---|---|---|
| Time-skipping test server + stub activities/children | Signal vs timer vs completion ordering, SLA timers, Continue-As-New, all in seconds | `ClaimLifecycleWorkflowTest`, `StpWorkflowTest` |
| **Gated activity stubs** (block an activity on a latch, inject a Signal/Update, release) | "Message arrives while X is in flight" races | `decisionAcceptedWhileTheTransitionIsStillRunningIsNotLost` |
| **Replay real histories** captured from the running system against new code | Non-determinism from code changes | `ReplayTest` + `src/test/resources/histories/*.json` |
| Scripted end-to-end scenarios against the real stack with chaos | Integration plus retries/idempotency | `make demo` with `MOCK_FAILURE_RATE` |
| Load test | Backlogs, timeouts, saturation | `make loadtest` |

Add a new replay fixture whenever a new workflow path ships. Exporting a history is one command
(`temporal workflow show -w <id> --output json`).

---

## 7. YAML/JSON workflow definition

### Is interpreting a definition inside a compiled workflow sound?

✅ **Yes, with the guardrails you listed.** All of them are implemented:

| Guardrail | Implementation |
|---|---|
| Registered operations only | `FlowInvariants.REGISTERED_STEPS`. An unknown step fails at **load time** (worker startup) and in `FlowRegistryTest` |
| No scripts, SQL or URLs | The schema only allows step names, stages, `interruptible`, `authorityLimitCents`, and `match` |
| Money authority and invariants in code | `MAX_AUTHORITY_CENTS` caps any YAML value. Payment stages can't be made interruptible. |
| Immutable + pinned | The flow is resolved **in an activity** at track start, so the result is recorded in history. `id@version` + content hash are stored in the `StpFlow` Search Attribute and the projection. Editing a YAML file never changes a running claim. |
| Variation by LOB / state / channel | `match` with most-specific-wins (`auto-cat-tx` beats `auto-default`) |
| Parallel steps | Steps in one stage run in parallel (`VENDOR_DISPATCH` + `ASSIGN_APPRAISAL`) |

### Versioning the interpreter itself

Flow *content* changes are safe: they apply to new track visits only. Interpreter *code* changes
are normal workflow code changes:

- Guard behaviour changes with `Workflow.getVersion` (example: the `estimate-backstop-poll` change in
  `awaitEstimate`, and `keep-early-adjuster-decision` in the parent), or ship them as a new Worker
  Deployment Version with PINNED children.
- **Replay-test** every interpreter change against captured histories of **every flow** in use
  (`ReplayTest`). CI should fail on any non-determinism.

### When is a DSL worth it?

| A DSL pays off when… | Stay compiled when… |
|---|---|
| Business owners change step order or variations often (weekly) | Variations change a few times a year |
| There are many variations (LOB × state × channel × company) that share the same steps | A handful of flows |
| Steps are stable, reusable building blocks | Each flow needs custom logic or branching on data |
| You can afford a validation pipeline and replay CI | No CI gate exists yet |

The scope here is deliberately narrow: ordering, parallelism, optional steps, authority. Avoid
growing it into conditions, loops or expressions. At that point you are writing a programming
language without the tooling, and compiled Java is the better DSL.

---

## Additional operational questions

| Question | Recommendation | Notes |
|---|---|---|
| AUTO_UPGRADE parent + PINNED children as the default? | ✅ Yes, and **implemented**: each track is its own Worker Deployment; the parent's worker defaults to AUTO_UPGRADE (kept replay-safe with `getVersion` + replay tests), the children's to PINNED. `deploy/deploy-setup.sh` promotes a build to current; production ramps first. | Cross-task-queue children start on the *target* queue's current version; confirm how this interacts with pinning **[confirm with Temporal]** |
| How long to keep old pinned versions? | Until the version is **drained** (no open pinned executions), plus as long as you need to **Query** closed executions on that version | Track a "drained" status before you decommission it |
| Do separate task queues and worker pools isolate STP from TSA/IS? | ✅ For worker capacity, yes: separate queues, deployments and autoscaling (one jar/role per deployment here). They **share the namespace's rate limits** (APS/RPS). If CAT load on one track must never throttle another, use a **separate namespace** per track group **[confirm limits with Temporal]**. | |
| Cloud Actions for Signals, Updates, Queries, SAs, describe(), history | Our understanding: Signals, Updates, Queries, SA upserts, timers, child/workflow starts, Continue-As-New and each activity attempt are Actions. `describe()`, history fetch and list/count are not Actions but are rate-limited. | **[confirm with Temporal]**. Measured per-claim numbers are in the README |
| claimId as a non-PII Search Attribute on parent and children | ✅ Yes, **if the claim number itself is not PII** (an internal number, not containing an SSN, name or policy holder data). Set on every execution (`ClaimId`) so `ClaimId = "X"` returns the whole tree. | SAs are **not** encrypted by a payload codec, and they are visible in the UI and to anyone with namespace read access |
| Encryption codec even with reference-only payloads? | ⚖ Recommended before production, not needed for the POC. Free-text fields (`reason`, `detail`, failure messages) can leak PII over time, and a codec makes "nothing readable leaves our VPC" a guarantee rather than a convention. | Must be **one codec implemented in Java and Python** (same algorithm, key-id metadata, compression flag), plus a **codec server** so the UI can decrypt for authorized users. Kept out of this repo so payloads stay readable in the demo. |
| Temporal Cloud connectivity (meeting notes) | API key or mTLS over **AWS PrivateLink** from the EKS VPC. Workers connect outbound only. | Env vars are the same in both languages (README) |

## Open items to take to Temporal

1. The exact Action accounting for Signal-With-Start, a rejected Update, a Query, and SignalExternal (forwarding).
2. Namespace APS limits for a CAT surge, and whether the STP/TSA/IS split should be by namespace.
3. Pinned-version behaviour for cross-task-queue children (Java parent → Python child).
4. Recommended retention and archival setup for a 7+ year claims audit requirement.
