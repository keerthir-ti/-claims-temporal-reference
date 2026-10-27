# Mock activities: what they mock and how to replace them

The workflows, workers and Temporal client in this repo are the deliverable. The external systems are
mocked **only at the activity layer**: each `Mock*` class implements the same activity interface
(Java) or activity names (Python) the real code will. Replace the mock classes, register the real
ones in the worker, and nothing else changes.

The mock activities read and write scripted data in the **mock-data service** (`mock-data/`), which
keeps one consistent "world" for both apps and simulates ClaimsPro's change feed (events to the bridge).

## Activities

| Activity | Track | System it stands for | Mock class | Real or mock |
|---|---|---|---|---|
| `beginStage`, `currentStageVersion` | Parent (Java) | ClaimsPro (stage fencing) | `MockLifecycleActivities` | mock |
| `recordProjection` | all (Java, called from Python too) | history database | `ProjectionActivitiesImpl` | **real, keep** |
| `fetchClaim` | STP | ClaimsPro | `MockStpActivities` | mock |
| `resolveFlow` | STP | flow definitions (`config/flows`) | `MockStpActivities` | **real logic, keep** |
| `initialContact` | STP | Comm Hub | `MockStpActivities` | mock |
| `verifyCoverage` | STP | Policy Gate | `MockStpActivities` | mock |
| `openReserve`, `issuePayment`, `finalizePayment`, `cancelPayment`, `closeClaim` | STP | ClaimsPro | `MockStpActivities` | mock |
| `assignAppraisal`, `requestEstimate`, `checkEstimate`, `listVendors`, `offerDispatch` | STP / dispatch | eClaims | `MockStpActivities` | mock |
| `loadClaim` | TSA, IS (Python) | ClaimsPro | `mock/tsa.py` | mock |
| `tsa.assessRisk` | TSA | triage AI model | `mock/tsa.py` | mock |
| `tsa.applyTriageRules` | TSA | rules (`config/triage-rules.yaml`) | `activities/triage.py` | **real, keep** |
| `is.analyzeCoverage` | IS | coverage AI model | `mock/investigation.py` | mock |
| `is.requestDocuments`, `is.documentsReceived` | IS | ImageRight | `mock/investigation.py` | mock |
| `is.recordCoverageOutcome` | IS | ClaimsPro | `mock/investigation.py` | mock |

Java mocks: `java/src/main/java/com/example/claims/temporal/activities/mock/`.
Python mocks: `python/claims/temporal/activities/mock/`.

## What the real implementations must keep

1. **Same signatures / names.** Java: implement `StpActivities` and `LifecycleActivities`. Python:
   register functions under the names above with the types in `activities/types.py`.
2. **Two tokens on every write**, from `StepContext` (Java) / `StepRef` (Python):
   `X-Stage-Version` (the system must reject writes from an older stage) and `Idempotency-Key`
   (`claimId:stageVersion:step`; a repeat must return the original result). If a system cannot do
   this natively, the activity checks first (look up by the key) before acting.
3. **Error mapping**: not found, business refusal and stale stage throw the non-retryable types in
   `ActivityErrors` / `activities/errors.py`; everything else is thrown as-is so Temporal retries it.
4. **Read current data inside the activity** from the reference passed in; never trust data carried
   in the workflow.

## Wiring

| Language | Where the mocks are registered |
|---|---|
| Java | `java/.../temporal/workers/Workers.java` (`new MockStpActivities(…)`, `new MockLifecycleActivities(…)`) |
| Python | `python/claims/temporal/workers/main.py` (`MockTsaActivities`, `MockInvestigationActivities`) |

## Scripted claims

Each file in `mock-data/fixtures/claims/` has a `claim` block (ClaimsPro data) and a `behaviour`
block grouped by the system that produces it:

| Fixture | Story | Behaviour used |
|---|---|---|
| CLM-1001 | Auto, straight through; vendor 1 declines; payment succeeds but reports failure once | `eClaims.vendors`, `eClaims.estimate`, `claimsPro.failPaymentAfterCommitOnce` |
| CLM-1002 | Home, STP; injury reported mid-flow → re-triage → MANUAL | `ai.risk`, `eClaims.estimate` |
| CLM-1003 | Coverage uncertain → IS; documents arrive later → STP | `ai.coverage`, `imageRight.documents` |
| CLM-1004 | High fraud score → SIU | `ai.risk` |
| CLM-1005 | Estimate above STP authority → MANUAL | `eClaims.estimate` |
| CLM-1006 | Catastrophe flow (TX, CAT channel); estimate event dropped | `eClaims.estimate.dropEvent` |

To add a scenario: copy a fixture, edit `claim` and `behaviour`, `make up`, then
`make seed FIXTURE=<name>`. Global failure injection: `MOCK_FAILURE_RATE`, `MOCK_LATENCY_MS` in `.env`.
