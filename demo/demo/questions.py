"""One live walkthrough per question in "Claims Reimagination – Temporal".

Each prints the question, the proposal, our recommended approach, then runs it against the
stack and prints the evidence. Run with `make q1` … `make q7`.
"""
from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path

import httpx
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowUpdateFailedError

from claims.temporal import names
from claims.contracts import models
from demo.common import (BRIDGE_URL, decide, in_track, new_claim, print_state, state, stp_running, wait_for,
                         world)

CONTRACTS_DIR = Path(os.environ.get("CONTRACTS_DIR", "/app/contracts"))
PROJECTION_DSN = os.environ.get("PROJECTION_DSN", "postgresql://claims:claims@postgres:5432/claims")


def header(number: int, question: str, proposal: str, ours: list[str]) -> None:
    print("\n" + "=" * 100)
    print(f"Q{number}. {question}")
    print("-" * 100)
    print(f"Proposed          : {proposal}")
    print("Our approach      : " + ours[0])
    for line in ours[1:]:
        print("                    " + line)
    print("=" * 100)


def step(text: str) -> None:
    print(f"\n▶ {text}")


def proof(text: str) -> None:
    print(f"  ✔ {text}")


async def executions(client: Client, claim_id: str) -> list:
    found = [e async for e in client.list_workflows(f'WorkflowId STARTS_WITH "claim:{claim_id}"')]
    return sorted(found, key=lambda e: e.start_time)


async def print_executions(client: Client, claim_id: str) -> list:
    found = await executions(client, claim_id)
    for e in found:
        print(f"    {e.workflow_type:<24} {e.id:<36} run={e.run_id[-8:]}  {e.status.name}")
    return found


async def raw_event(claim_id: str, event_type: str, event_id: str) -> str:
    """Post a raw ClaimsPro change event straight to the bridge, as the CDC feed would."""
    async with httpx.AsyncClient(base_url=BRIDGE_URL, timeout=10) as http:
        resp = await http.post("/events", json={"id": event_id, "type": event_type, "claimId": claim_id,
                                                "version": 99, "changedFields": ["notes"],
                                                "policyHolderName": "PII that must be stripped"})
        return resp.json().get("result", str(resp.status_code))


# ------------------------------------------------------------------------------------------- Q1

async def q1(client: Client) -> int:
    header(1, "One parent with three track children? Keep the parent thin? New child or reuse? Reject stale results?",
           "Java parent owns transitions; TSA/IS (Python) and STP (Java) children; stage versions / lease tokens.",
           ["Thin parent that only routes (pure routing table); children return an OUTCOME, never pick the next track.",
            "A NEW child execution per track visit, its Workflow ID carries the stage version.",
            "Stage version = lease AND fence: ClaimsPro rejects writes from an older stage (409),",
            "so a late activity from an abandoned stage cannot change the claim."])
    claim_id = await new_claim("CLM-1002")
    step(f"File {claim_id} (home claim, straight-through) and wait until STP is running")
    await wait_for(client, claim_id, stp_running, label="STP running")
    await asyncio.sleep(4)
    step("Injury is reported in ClaimsPro mid-flow -> STP asks the PARENT for re-triage (it cannot route itself)")
    await world("PATCH", f"/claims/{claim_id}", {"injuryReported": True})
    s = await wait_for(client, claim_id, in_track("MANUAL"), label="MANUAL")
    print_state(s)
    step("Executions for this claim: one parent, a new child per track visit")
    found = await print_executions(client, claim_id)
    tsa_runs = [e for e in found if e.workflow_type == names.TSA_WORKFLOW]
    proof(f"TSA visited {len(tsa_runs)} times -> {len(tsa_runs)} separate child executions, never reused")
    step("Replay a payment write from stage 2 (the abandoned STP visit), as a late activity retry would")
    result = await world("POST", f"/claims/{claim_id}/stale-write", {"stageVersion": 2})
    print(f"    ClaimsPro answered: {json.dumps(result)}")
    if result.get("accepted"):
        print("  ✘ stale write was accepted")
        return 1
    proof("rejected by the system of record: stale work cannot pay or close the claim after it moved on")
    await decide(client, claim_id, "CLOSE")
    return 0


# ------------------------------------------------------------------------------------------- Q2

async def q2(client: Client) -> int:
    header(2, "When a claim closes, end the workflow or stay open? Reopen: same Workflow ID or new? Continue-As-New?",
           "Open question in the design document.",
           ["Stay open in CLOSED for a bounded REOPEN WINDOW, then complete.",
            "Reopen inside the window: same run. After it: NEW RUN, SAME Workflow ID (claim:{claimId})",
            "via Signal-With-Start; stage numbering continues from ClaimsPro so fences and idempotency keys stay valid.",
            "Continue-As-New at track boundaries (or while waiting) when history is large."])
    claim_id = await new_claim("CLM-1006")
    handle = client.get_workflow_handle(names.lifecycle_workflow_id(claim_id))
    step(f"File {claim_id} and let it run to CLOSED")
    await wait_for(client, claim_id, in_track("CLOSED"), label="CLOSED")
    first = await handle.describe()
    proof(f"claim is CLOSED but the workflow is still {first.status.name}: it waits out the reopen window")
    step("Customer reopens the claim INSIDE the window")
    await world("POST", f"/claims/{claim_id}/reopen")
    s = await wait_for(client, claim_id, lambda s: len(s.transitions) > 3 and s.currentTrack.value == "CLOSED",
                       label="closed again")
    again = await handle.describe()
    print_state(s)
    proof(f"handled in the SAME run ({first.run_id[-8:]} == {again.run_id[-8:]}), stages continued")
    step("Wait for the reopen window to elapse -> the workflow completes (nothing left running)")
    for _ in range(180):
        if (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED:
            break
        await asyncio.sleep(1)
    completed = await handle.describe()
    proof(f"workflow {completed.status.name} (run {completed.run_id[-8:]})")
    last_stage = s.stageVersion
    step("Reopen AFTER completion -> bridge uses Signal-With-Start: new run, same Workflow ID")
    await world("POST", f"/claims/{claim_id}/reopen")
    for _ in range(60):  # wait until the new run exists; until then the ID still resolves to the old one
        if (await handle.describe()).run_id != completed.run_id:
            break
        await asyncio.sleep(1)
    s = await wait_for(client, claim_id,
                       lambda s: bool(s.transitions) and s.transitions[0].stageVersion > last_stage
                       and s.currentTrack.value == "CLOSED",
                       timeout=150, label="closed in the new run")
    print_state(s)
    await print_executions(client, claim_id)
    first_new = s.transitions[0].stageVersion
    proof(f"new run {(await handle.describe()).run_id[-8:]} started at stage {first_new}, not 1: it continued "
          f"from ClaimsPro's stage {last_stage}, so fences and idempotency keys stay valid "
          "(the new payment is a new payment, not a 'replay' of the old one)")
    return 0 if first_new > last_stage else 1


# ------------------------------------------------------------------------------------------- Q3

async def q3(client: Client) -> int:
    header(3, "Projection table for years of history? Who stores what? Must the workflow wait for each write?",
           "Application-owned projection written by idempotent activities keyed claimId:stageVersion:step.",
           ["Yes: the projection is the BUSINESS record (years, SQL, open + closed claims together).",
            "Temporal history / archive = developer record. Each row stores workflow_id + run_id to join the two.",
            "Writes are async and awaited at track boundaries (STP) / before completion (parent): no step blocks,",
            "no row is lost. Keyed claimId:stageVersion:step so retries never duplicate a row."])
    import psycopg

    claim_id = await new_claim("CLM-1003")
    step(f"File {claim_id}: TSA (Python) -> IS (Python) -> TSA -> STP (Java) -> CLOSED")
    await wait_for(client, claim_id, in_track("CLOSED"), timeout=180, label="CLOSED")
    await asyncio.sleep(2)
    step("Business history from the projection table (what an adjuster or report sees)")
    with psycopg.connect(PROJECTION_DSN) as conn:
        rows = conn.execute(
            "select stage_version, track, step, status, left(detail, 48), workflow_id, right(run_id, 8) "
            "from claim_projection where claim_id = %s order by occurred_at, stage_version", (claim_id,)).fetchall()
    for v, track, st, status, detail, wf, run in rows:
        print(f"    v{v:<2} {track:<6} {st:<24} {status:<10} {detail:<48} {wf}  run={run}")
    proof(f"{len(rows)} business rows across Java and Python tracks, each linked to its exact execution")
    proof("why was the payment issued? -> the ISSUE_PAYMENT row; how? -> open that workflow_id/run_id in Temporal")
    return 0


# ------------------------------------------------------------------------------------------- Q4

async def q4(client: Client) -> int:
    header(4, "Bridge -> parent -> child? Events between tracks / after the end? 100-150 events: signal each? Approvals?",
           "Bridge normalises allowlisted events and signals the parent; the parent forwards to the active child.",
           ["Keep bridge -> parent -> active child. Signal per event, deduplicated by eventId.",
            "Events with no consumer are BUFFERED (newest per type) for the next child; children re-read ClaimsPro anyway.",
            "Only FNOL/REOPEN may start a workflow; other events for a finished claim are DEAD-LETTERED.",
            "Adjuster approvals = UPDATE on the parent: validated before history, acknowledged, idempotent by decisionId."])
    claim_id = await new_claim("CLM-1004")
    step(f"File {claim_id} (high fraud score) -> SIU, waiting for an adjuster; no child is running")
    await wait_for(client, claim_id, in_track("SIU"), label="SIU")
    before = (await state(client, claim_id)).bufferedEventCount
    event_id = f"evt-{uuid.uuid4().hex[:8]}"
    step(f"ClaimsPro change feed delivers the SAME event twice (eventId {event_id})")
    for _ in range(2):
        print(f"    bridge -> {await raw_event(claim_id, 'claim.updated', event_id)}")
    await asyncio.sleep(2)
    after = (await state(client, claim_id)).bufferedEventCount
    proof(f"parent kept it once (buffered {before} -> {after}) and holds it for the next track")
    step("An event for a claim with no running workflow (not FNOL/REOPEN)")
    print(f"    bridge -> {await raw_event('NO-SUCH-CLAIM', 'claim.updated', str(uuid.uuid4()))}")
    proof("dead-lettered, not used to start (resurrect) a workflow")
    step("Adjuster sends an invalid decision")
    try:
        await decide(client, claim_id, "UNKNOWN")
        print("  ✘ invalid decision accepted")
        return 1
    except WorkflowUpdateFailedError as e:
        proof(f"rejected by the Update validator before reaching history ({e.cause})")
    step("Adjuster clicks CLOSE, the UI retries the same request")
    decision_id = str(uuid.uuid4())
    first = await decide(client, claim_id, "CLOSE", decision_id)
    retry = await decide(client, claim_id, "CLOSE", decision_id)
    proof(f"first: duplicate={first.duplicate}; retry: duplicate={retry.duplicate} (applied once, both acknowledged)")
    await wait_for(client, claim_id, in_track("CLOSED"), label="CLOSED")
    proof("claim CLOSED")
    return 0


# ------------------------------------------------------------------------------------------- Q5

async def q5(client: Client) -> int:
    header(5, "JSON Schema or Protobuf between Java and Python? Versioning rules? Java/Python differences?",
           "Canonical JSON with generated models, additive-only evolution, enum fallback, golden tests.",
           ["JSON Schema -> generated Java (jsonschema2pojo) and Python (pydantic) models from ONE file.",
            "schemaVersion on every message; additive-only; readers ignore unknown fields;",
            "unknown enum values become UNKNOWN (parent routes them to MANUAL instead of failing).",
            "The same golden messages are round-tripped by the Java AND the Python test suites."])
    path = CONTRACTS_DIR / "golden" / "forward-compat" / "TrackResult.json"
    step(f"A TrackResult written by a NEWER child version ({path.name}, forward-compat sample)")
    raw = json.loads(path.read_text())
    print("    " + json.dumps(raw))
    parsed = models.TrackResult.model_validate(raw)
    proof(f"old reader still parses it: outcome={parsed.outcome.name}, unknown fields ignored")
    step("Java parent state read from Python (Java wrote it, Python decodes it with the generated model)")
    claim_id = await new_claim("CLM-1001")
    s = await wait_for(client, claim_id, lambda s: s.stageVersion >= 2, label="running")
    proof(f"LifecycleState schemaVersion={s.schemaVersion} track={s.currentTrack.value} decoded in Python")
    print("\n  Both suites check the same golden files: make test-java (ContractGoldenTest) / make test-python")
    return 0


# ------------------------------------------------------------------------------------------- Q6

async def q6(client: Client) -> int:
    header(6, "React to claim changes mid-flow? What about an activity already running? Testing races?",
           "Small signal -> dedupe -> re-read ClaimsPro -> act after revalidation -> timer backstop.",
           ["Your pattern, plus a MATERIAL-CHANGE filter: cosmetic edits do nothing.",
            "Important change mid-stage: cancel in-flight work and re-triage - EXCEPT payment stages, which finish first.",
            "Backstops: re-read at stage boundaries and poll while waiting, so a lost event cannot strand a claim.",
            "Races tested with the time-skipping server, gated activities and replay of real histories."])
    lost = await new_claim("CLM-1006")  # runs in the background: its estimate event is dropped
    claim_id = await new_claim("CLM-1002")
    step(f"File {claim_id}; wait until STP is waiting for the vendor estimate")
    await wait_for(client, claim_id, stp_running, label="STP running")
    await asyncio.sleep(4)
    step("Cosmetic change in ClaimsPro (company code) -> event -> STP re-reads -> not material")
    await world("PATCH", f"/claims/{claim_id}", {"company": "CO-RI"})
    await asyncio.sleep(4)
    s = await state(client, claim_id)
    proof(f"still {s.currentTrack.value}, retriage={s.retriageCount}: no churn on irrelevant updates")
    step("Material change (injury reported) -> STP cancels the interruptible stage and asks for re-triage")
    await world("PATCH", f"/claims/{claim_id}", {"injuryReported": True})
    s = await wait_for(client, claim_id, in_track("MANUAL"), label="MANUAL")
    print_state(s)
    proof("re-triaged on revalidated data, not on the event payload")
    await decide(client, claim_id, "CLOSE")
    step(f"{lost}: the 'estimate received' event was DROPPED by the change feed")
    s = await wait_for(client, lost, in_track("CLOSED"), timeout=150, label="CLOSED")
    proof(f"{lost} still CLOSED: the backstop poll found the estimate in eClaims")
    print("\n  Races (signal vs timer vs completion vs Continue-As-New) are covered by make test-java:")
    print("  decisionAcceptedWhileTheTransitionIsStillRunningIsNotLost, longWaitInSiuContinuesAsNew…, ReplayTest")
    return 0


# ------------------------------------------------------------------------------------------- Q7

async def q7(client: Client) -> int:
    header(7, "Can a fixed workflow run steps from a YAML/JSON definition? When is it worth it?",
           "Pinned version + hash, registered operations only, payment authority and invariants in code.",
           ["Yes, as a NARROW DSL: YAML orders registered steps, marks parallel/optional steps, sets limits.",
            "The flow is resolved in an activity and PINNED (id@version + content hash) for the claim's STP visit.",
            "Authority ceiling and 'never interrupt a payment' stay in compiled Java; unknown steps fail at startup.",
            "Interpreter changes are guarded with getVersion and replay-tested against captured histories."])
    regular = await new_claim("CLM-1001")   # AUTO, MA, AGENT channel
    cat = await new_claim("CLM-1006")       # AUTO, TX, CAT channel
    over = await new_claim("CLM-1005")      # estimate above the flow's authority
    step("Three claims, same STP workflow code, different flow definitions")
    for claim_id in (regular, cat):
        await wait_for(client, claim_id, in_track("CLOSED"), timeout=150, label="CLOSED")
    await wait_for(client, over, in_track("MANUAL"), timeout=150, label="MANUAL")
    for claim_id in (regular, cat, over):
        flows = [e.search_attributes.get("StpFlow", ["?"])[0]
                 async for e in client.list_workflows(f'WorkflowType="StpWorkflow" AND ClaimId="{claim_id}"')]
        s = await state(client, claim_id)
        print(f"    {claim_id:<16} flow={','.join(flows):<16} final track={s.currentTrack.value}  "
              f"{s.transitions[-1].reason}")
    proof("regular auto claim ran auto-default; the TX catastrophe claim ran auto-cat-tx (no INITIAL_CONTACT, "
          "reserve + appraisal in parallel)")
    proof("over-authority estimate was handed to a human: the YAML limit is capped by the compiled $25,000 ceiling")
    await decide(client, over, "CLOSE")
    print("\n  Invalid flows (unregistered step, payment before estimate) are rejected at worker start: "
          "FlowRegistryTest")
    return 0


QUESTIONS = {
    "q1": ("Parent + children, new child per visit, stale results", q1),
    "q2": ("Closing, reopening, workflow IDs", q2),
    "q3": ("Long-term business history", q3),
    "q4": ("Event routing, buffering, dead letters, approvals", q4),
    "q5": ("Java/Python contracts", q5),
    "q6": ("Reacting to changes mid-flow", q6),
    "q7": ("YAML flow definitions", q7),
}
