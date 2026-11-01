# Claims Orchestration on Temporal

Two production designs for the claims platform's Temporal layer. Both run the same claim process (triage,
investigation, straight-through processing, human decisions, payment, close, reopen) on the same production
baseline; they differ in how the process is split into Temporal workflows. Each is built, tested and runnable on
its own, so the choice can be made on evidence.

Both use only the components in the client's architecture (ClaimsPro, eClaims, ImageRight, Policy Gate, Comm Hub,
the event bridge, Kong, Temporal Cloud, EKS workers, the projection table, S3) and mock the claims systems at the
activity boundary only: replacing a mock with the real system changes the worker that registers it, never a workflow.

| Folder | Design | Temporal pattern |
|---|---|---|
| [parent-child-orchestration/](parent-child-orchestration/) | Parent–Child Orchestration | A claim workflow with a child workflow per track visit and per payment |
| [entity-workflow/](entity-workflow/) | Entity Workflow | One workflow per claim; tracks are modules inside it, their work runs as activities |

## The claim process both designs run

```
claim created ─► TRIAGE ─► STP ───────────────► CLOSED ─► (reopen) ─► REOPEN_REVIEW ─► TRIAGE
                   │  ▲     │ material change ▲
                   │  └─────┘ (re-triage)     │
                   ├─► INVESTIGATION ─► TRIAGE │
                   ├─► SIU (clear / deny) ─────┤
                   └─► MANUAL ─► ESCALATION ───┘
```

Every stage change increments a stage version kept outside Temporal; every write carries it, so a late result from
an older stage is refused. STP runs the claim's flow (contact, coverage, reserve, dispatch and appraisal, estimate,
payment, close) from configuration. People decide where the authority matrix says so, through validated Updates
with deadlines and escalation.

## Parent–Child Orchestration

**How it works.** `ClaimLifecycleWorkflow` (`claim:{claimId}`) owns the stage and the cross-track rules. Each visit
to a track starts a new child workflow on that track's queue and worker: `TriageWorkflow` and
`InvestigationWorkflow` on the AI workers, `StpWorkflow` on the STP worker. Each payment is a `PaymentWorkflow`, a
child of STP that is abandoned (not cancelled) when STP ends, so it always finishes. The event bridge signals the
parent, which forwards events to the open child or holds them for the next one. A human decision is an Update sent
to the workflow that waits for it; the ClaimsPro task carries that workflow's ID.

**Workflows per straight-through claim:** four (claim, triage, STP, payment), each with its own short history.

| Pros | Cons |
|---|---|
| Standard Temporal pattern; each track team owns and releases its own workflow | Four workflow types to version (children pinned to their build, parent patched) |
| One owner of the stage; tracks cannot hand off to each other or loop without limit | Every forwarded event costs one action; 100–150 events per claim |
| Short histories; a defect in one track stays in that track | A new child per visit adds actions and a short start-up delay at each stage change |
| Payment has its own workflow, audit trail and lifecycle | The ClaimsPro task must carry the waiting workflow's ID, not just the claim ID |
| Children pinned to the build that started them: track releases never disturb running visits | More executions to follow when tracing one claim |

## Entity Workflow

**How it works.** `ClaimWorkflow` (`claim:{claimId}`) is the only claim workflow and runs the claim for its whole
life. A stage machine routes between modules: `TriageModule`, `InvestigationModule` and `StpModule` are plain code
inside the workflow, and their work runs as activities on each track's own queue and worker (the AI activities on
the AI workers, the STP activities on the STP worker). STP stages run in order and the steps of a stage in parallel;
a material change cancels the running stage at once. Payment runs in a region of the workflow outside every
cancellation scope. Every event and every decision goes to the same workflow ID.

**Workflows per straight-through claim:** one, with one history (bounded by Continue-As-New between stages).

| Pros | Cons |
|---|---|
| Simplest model: one claim, one workflow, one ID for every event and decision | One workflow type holds all track logic: kept in modules, reviewed and replay-tested as one |
| No forwarding or decision relay; nothing to keep in sync between workflows | Track teams write activities, not workflows |
| Fewest actions per claim | One history grows with every track: Continue-As-New between stages |
| Activity workers release without any workflow versioning | Every workflow change must be patched, as the workflow lives for the whole claim |
| Cross-track rules, re-triage and payment in one place | A workflow defect affects the whole claim, not one track |

## Production baseline shared by both

| Concern | How both designs handle it |
|---|---|
| Data | Claim data stays in ClaimsPro; Temporal holds IDs, stage, timers and decisions; business history in the projection table |
| No data loss | Awaited, idempotent history writes; record verified complete before a claim workflow completes; nightly archive to S3 deletes only rows it copied and verified; workflow histories exported to S3, with a nightly fallback while Temporal still retains them |
| S3 and retention | Temporal Cloud has no Archival; Workflow History Export is the S3 path; namespace retention at least 30 days; bucket versioning, object lock, client KMS key |
| Money | Payment key looked up before create; only the unpaid balance is paid; a created payment always closes; never stopped halfway; payment-authority ceiling in code |
| Retries | Backoff within a retry window, then an operations task: a system outage delays a claim, never fails it; business failures not retried; AI capped at 5 attempts |
| Signals and Updates | Events by Signal-With-Start and Signal, deduplicated by event ID; decisions by validated Update, decision ID as Update ID |
| People | Role, authority, four eyes, required reasons, deadlines and escalation from `config/decision-policy.yaml` |
| Security | Outbound mTLS over PrivateLink; AES-256-GCM payload encryption, identical in Java and Python, with key rotation; IDs only in workflow IDs and search attributes |
| Contracts | Protocol Buffers; golden payloads tested in both languages; unknown fields tolerated during rolling releases |
| Releases | Worker Versioning; workflow changes behind `Workflow.getVersion`, proven by replay tests of recorded histories |
| Load | ClaimsPro protected by a server-side rate limit on its task queue; workers scale per queue |

## Side by side

| | Parent–Child Orchestration | Entity Workflow |
|---|---|---|
| Orchestration code written by | Each track team (its own workflow) | The lifecycle team; track teams write activities |
| Events | Parent forwards to the open child | Handled in place |
| Human decisions | Update on the waiting workflow | Update always on `claim:{claimId}` |
| Versioning | Parent Auto-Upgrade with patching; children Pinned | One workflow, Auto-Upgrade with patching |
| History | Split across child executions | One per claim, Continue-As-New between stages |
| Blast radius of a workflow defect | One track | The whole claim |
| Actions per straight-through claim (measured) | About 67 | About 55 |
| 200 claims at 20/s (measured locally) | All closed once in 85 s | All closed once in 59 s |
| Scenarios verified end to end | 10 of 10 | 10 of 10 |

## Choosing

Choose **Parent–Child Orchestration** when separate teams must own, release and operate their track workflows
independently, or when a track's lifecycle must be isolated from the claim's.

Choose **Entity Workflow** when one team owns the claim process and simplicity, lower Temporal cost and one view of
each claim matter most.

Each folder's README is the full design of that approach; its `design/` document is the version for review.

```bash
cd parent-child-orchestration   # or entity-workflow
make verify                     # tests, stack, ten claim scenarios, catastrophe load test
```
