# Claims Orchestration on Temporal — reference solution

The claims team asked seven questions in **Claims Reimagination – Temporal** before locking their claims
orchestration design. This repository is **the recommended answer, implemented**: the Temporal
workflows, child workflows, activities and workers built the way we recommend. It is not a copy of
the current design. Where we agree with their proposal we implement it; where we would do
it differently, or need to add something to make it safe, the code does that and
[docs/SOLUTIONS.md](docs/SOLUTIONS.md) says why.

It is based on three inputs: the design document, the kickoff call transcript and its summary.
The existing systems (ClaimsPro, Policy Gate, Comm Hub, eClaims, ImageRight, the AI models) are
**mocked at the activity layer**. The claims team keeps the workflows, workers and client code, and
replaces the `Mock*` activity classes with real ones.

## The seven questions, answered live

Each answer runs against the stack and prints the evidence (`make up` first).

| # | Question | Our answer (implemented) | Show it |
|---|---|---|---|
| 1 | One parent + three track children? Keep the parent thin? New child or reuse? Stale results? | Parent only routes; children return outcomes; **new child per track visit**; stage version is a lease **and a fence enforced by ClaimsPro** | `make q1` |
| 2 | Close: end or stay open? Reopen: same Workflow ID? Continue-As-New? | **Reopen window**, then complete; reopen later = **new run, same Workflow ID**, stage numbering continued from ClaimsPro; Continue-As-New at boundaries and during long waits | `make q2` |
| 3 | Projection table for years? Who stores what? Wait for each write? | Projection = business record, linked to `workflow_id` + `run_id`; writes async, **awaited at track boundaries**; idempotent key | `make q3` |
| 4 | Bridge → parent → child? Awkward-timing events? 100–150 events? Approvals? | Keep the path; **dedupe + buffer + dead-letter**; children re-read; approvals = **validated, idempotent Update on the parent** | `make q4` |
| 5 | JSON Schema or Protobuf? Versioning? Java/Python differences? | One JSON Schema → generated Java + Python models; `schemaVersion`; additive-only; **UNKNOWN fallback**; golden tests in both languages | `make q5` |
| 6 | React to changes mid-flow? In-flight activities? Race testing? | Your pattern + **material-change filter**; cancel in-flight work **except payment stages**; backstop polls; race + replay tests | `make q6` |
| 7 | Run steps from YAML/JSON? Worth it? | **Narrow DSL**: registered steps only, flow **pinned by version + hash**, authority and invariants in compiled code | `make q7` |
| — | Deployment questions | **Worker Deployment Versioning** (parent AUTO_UPGRADE, children PINNED), separate queues per track, non-PII Search Attributes | `make up` |

## Repository layout

```
java/                     Java application (Claims Lifecycle + STP), as in the deployment diagram
  src/main/java/com/example/claims/
    temporal/
      workflows/          lifecycle/ (parent + routing table) · stp/ (STP, vendor dispatch, flows/)
      activities/         activity interfaces + payload types · ProjectionActivitiesImpl (real)
        mock/             MockLifecycleActivities, MockStpActivities   ← replaced by real code
      workers/            worker registration per deployment, Worker Versioning
      client/             Temporal client, data converter, Claims Event Bridge
    db/                   projection store (JDBC)
python/                   Python application (TSA + IS)
  claims/temporal/
      workflows/          tsa.py, investigation.py
      activities/         activity contract + types · triage.py (real rules engine)
        mock/             MockTsaActivities, MockInvestigationActivities   ← replaced by real code
      workers/            main.py (one process per track), Worker Versioning
      client/             connection, JSON logging
contracts/                JSON Schema (single source for Java + Python models) + golden messages
config/                   STP flow definitions (YAML) + triage rules
db/                       Postgres schema: claim_projection, claim_current
mock-data/                scripted claims + the mock-data service the mock activities read from
deploy/                   promotes worker versions (Worker Versioning)
observability/            Filebeat, Metricbeat, Kibana setup (ELK)
demo/                     make q1…q7 walkthroughs, scenario runner, load test, Actions estimator
docs/                     SOLUTIONS, DECISIONS, MOCK-ACTIVITIES, OBSERVABILITY
```

Workflows and child workflows: 1 parent (`ClaimLifecycleWorkflow`, Java), 3 children (`TsaWorkflow`
and `InvestigationWorkflow` in Python, `StpWorkflow` in Java), 1 grandchild
(`VendorDispatchWorkflow`, Java). Only the parent changes a claim's track; every track visit starts a
new child. MANUAL, SIU, ESCALATION and CLOSED are parent states, decided by an adjuster Update.

## Quick start

**Prerequisites: Docker and `make` only.** Give the Docker VM ≥ 6 GB (≥ 10 GB with ELK).

```bash
make up           # build + start Temporal, Java + Python apps, mock data, Postgres; promote worker versions
make q1           # … make q7: one answer at a time   |   make questions: all seven (~8 min)
make demo         # all 6 scripted claims end to end, every outcome checked (~2 min)
make help         # every target
```

Temporal UI: <http://localhost:8233> (search `ClaimId STARTS_WITH "CLM-1003"` to see the parent and
its children). ELK: `make elk-up`, then Kibana on <http://localhost:5601>. On a small machine run
`make elk-down` before `make demo` or `make loadtest`.

Interactive commands (`make seed FIXTURE=CLM-1002` prints a fresh claim id):

```bash
make state ID=…  ·  make patch ID=… FIELDS="injuryReported=true"  ·  make decide ID=… ACTION=CLOSE
make history ID=…  ·  make ledger ID=…  ·  make reopen ID=…  ·  make loadtest CLAIMS=1000 RATE=50
make actions ID="…"  ·  make pilot | make full  ·  make logs S=stp-worker  ·  make down | make clean
```

## Replacing the mocks with real activities

Workflows call activities only through their interfaces (Java) or registered names (Python), so a
real implementation drops in without touching workflow code:

| Mock (replace) | Interface / names it implements | Real system |
|---|---|---|
| `java/.../activities/mock/MockStpActivities` | `StpActivities` | ClaimsPro, Policy Gate, Comm Hub, eClaims |
| `java/.../activities/mock/MockLifecycleActivities` | `LifecycleActivities` | ClaimsPro (stage fencing) |
| `python/.../activities/mock/tsa.py` | `loadClaim`, `tsa.assessRisk` | ClaimsPro, triage AI |
| `python/.../activities/mock/investigation.py` | `is.*` activities | AI coverage model, ImageRight, ClaimsPro |

Then register the real classes in `java/.../temporal/workers/Workers.java` and
`python/claims/temporal/workers/main.py`. What the real code must keep is listed in
[docs/MOCK-ACTIVITIES.md](docs/MOCK-ACTIVITIES.md): stage-version and idempotency tokens on every
write, and the error mapping that decides what Temporal retries.

## Moving to Temporal Cloud and production

- **Connection:** both apps read the same variables: `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, and
  either `TEMPORAL_API_KEY` or `TEMPORAL_TLS_CERT` / `TEMPORAL_TLS_KEY`. Workers connect outbound
  only, through AWS PrivateLink.
- **Search Attributes** to register on the namespace first: `ClaimId`, `ClaimTrack`, `StpFlow`
  (Keyword), `StageVersion`, `RetriageCount` (Int). Without them every upsert fails.
- **Releases:** start workers with a new `BUILD_ID`, then promote it (`deploy/deploy-setup.sh` locally;
  ramp in production). Parent changes are guarded with `Workflow.getVersion` and must pass the replay
  tests; capture a history for each new path with
  `temporal workflow show -w <id> --output json > java/src/test/resources/histories/<name>.json`.
- **Measured locally** (dev server, 5% injected failures): 300 claims at 30/s all settled in 81 s, none
  lost; about 43–72 Temporal Actions per claim plus 1–2 per inbound event (`make actions ID=…`).
  Confirm Cloud counting rules with Temporal before budgeting.
- **Not in the demo, needed for production:** a payload encryption codec shared by Java and Python,
  and a durable change-feed consumer in place of the HTTP event bridge (see `EventBridge.java`).

## Tests

```bash
make test-java     # 36: routing, flow validation, workflows on the time-skipping server, race and
                   #     Continue-As-New regressions, replay of captured histories, golden contracts
make test-python   # 17: golden contracts, triage rules, TSA/IS workflows, AI retry cap
```

## Documents

| Document | Purpose |
|---|---|
| [docs/SOLUTIONS.md](docs/SOLUTIONS.md) | Per question: what was proposed, our solution, what we changed or added, where in the code, how to show it |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Full reasoning: options with pros and cons, thresholds, pitfalls |
| [docs/MOCK-ACTIVITIES.md](docs/MOCK-ACTIVITIES.md) | Every activity, what it mocks, how to replace it |
| [docs/OBSERVABILITY.md](docs/OBSERVABILITY.md) | ELK: tracing one claim across Java + Python, SDK metrics, alerts |
