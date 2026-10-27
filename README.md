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

## Getting started

**You need:** Docker Desktop (or Docker Engine) with Compose v2 (`docker compose version`), `make`,
and git. No Java, Maven or Python. Give Docker at least **6 GB** of memory (Docker Desktop →
Settings → Resources), or 10 GB if you also start ELK. macOS: `make` comes with the Xcode command
line tools (`xcode-select --install`). Windows: run everything inside WSL2.

**Ports used on your machine:** 8233 (Temporal UI), 17233 (Temporal), 15432 (Postgres), 8090 (mock
data), 8085 (event bridge); 9200 and 5601 only with ELK. If one is taken, change it in `.env`
(created from `.env.example` on the first `make up`).

```bash
git clone git@github.com:keerthir-ti/-claims-temporal-reference.git claims-temporal-reference
cd claims-temporal-reference
make up           # first run 5–10 min (downloads images, Maven and pip packages); later runs ~1 min
make ps           # every service "Up"; temporal and postgres "healthy"
make demo         # 6 scripted claims end to end: expect 6 × PASS (~2 min)
```

`make up` ends by printing four `current version … -> v1` lines and the URLs. If you see those and
`make demo` prints 6 × PASS, everything works. `make help` lists every target.

Temporal UI: <http://localhost:8233>. Search `ClaimId STARTS_WITH "CLM-1003"` to see a claim's
parent and child workflows. ELK: `make elk-up`, then Kibana on <http://localhost:5601>
(3–5 min to start). On a laptop with less than 10 GB for Docker, keep ELK off while demoing.

## Presenting the demo (about 25 minutes)

Run `make up` and `make demo` once beforehand so images are built and the stack is warm. During the
session keep the Temporal UI open next to the terminal; every walkthrough prints its claim ids.

| Step | Run | What to point out |
|---|---|---|
| 1 | Open this README | The question table above and the repository layout: workflows and workers are the deliverable, external systems are mocked in `activities/mock/` only |
| 2 | `make q1` | One thin parent, a new child per track visit (shown in the executions list), and a stale write rejected with 409 by the system of record |
| 3 | `make q2` (~2 min) | Claim stays open during the reopen window; a later reopen starts a new run with the same Workflow ID and continues at stage 7, not 1 |
| 4 | `make q3` | Business history rows, each linked to its workflow and run ID |
| 5 | `make q4` | Duplicate event kept once, stray event dead-lettered, invalid approval rejected, retried approval applied once |
| 6 | `make q5` | A message from a newer version still decodes; Java state read from Python |
| 7 | `make q6` | Cosmetic change ignored, injury causes re-triage, lost estimate event recovered by the backstop poll |
| 8 | `make q7` | Same STP code, different flow per claim type; estimate over the limit handed to a person |
| 9 | [docs/SOLUTIONS.md](docs/SOLUTIONS.md) | What was changed or added compared with the proposal, question by question |

Optional extras: `make loadtest CLAIMS=300 RATE=30` (surge with injected failures), `make test`
(both test suites), and the interactive commands below for live questions from the audience.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `make up` fails with "port is already allocated" | Another service uses one of the ports above | Change the port in `.env` and run `make up` again |
| Claims stay in TSA and never move | Worker versions were not promoted | `docker compose run --rm deploy-setup` |
| Every claim shows `state=None` or the demo times out | Docker is out of memory (often because ELK is running) | `make elk-down`, or give Docker more memory, then retry |
| A claim in progress suddenly goes to MANUAL | The `mock-data` container was restarted and lost its in-memory data | File a new claim (`make seed`); don't restart `mock-data` during a demo |
| `make q1` … `q7` cannot connect | The stack is not running | `make ps`, then `make up` |
| Start completely fresh | — | `make clean && make up` |

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
