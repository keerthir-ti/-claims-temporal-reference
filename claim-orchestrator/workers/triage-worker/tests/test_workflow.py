"""TriageWorkflow on the time-skipping test server. The Java-hosted activities (ClaimsPro, projection,
policy) are faked here under the same names and task queues."""
import uuid

import pytest
from temporalio import activity
from temporalio.client import WorkflowUpdateFailedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from claims.v1 import decisions_pb2, projection_pb2, systems_pb2, tracks_pb2
from claims_runtime import names
from triage_worker.rules import TriageRules
from triage_worker.workflow import TriageWorkflow

TASKS: dict[str, decisions_pb2.OpenDecision] = {}
RISK = systems_pb2.RiskAssessment()


@activity.defn(name="Policy.load")
async def load_policy(_: decisions_pb2.PolicyRequest) -> decisions_pb2.DecisionPolicy:
    p = decisions_pb2.DecisionPolicy(retry_window_seconds=60, triage_confidence_threshold=0.6)
    p.rules["TRIAGE_REVIEW"].CopyFrom(decisions_pb2.DecisionRule(role="triage-adjuster", deadline_seconds=3600))
    return p


@activity.defn(name="Projection.record")
async def record(_: projection_pb2.ProjectionEvent) -> systems_pb2.Done:
    return systems_pb2.Done(ok=True)


@activity.defn(name="ClaimsPro.readClaim")
async def read_claim(ref: systems_pb2.ClaimRef) -> systems_pb2.ClaimSnapshot:
    return systems_pb2.ClaimSnapshot(claim_id=ref.claim_id, status="OPEN")


@activity.defn(name="ClaimsPro.createTask")
async def create_task(r: systems_pb2.TaskRequest) -> systems_pb2.TaskRef:
    TASKS[r.decision.decision_id] = r.decision
    return systems_pb2.TaskRef(task_id=f"T-{len(TASKS)}")


@activity.defn(name="ClaimsPro.closeTask")
async def close_task(_: systems_pb2.CloseTaskRequest) -> systems_pb2.Done:
    return systems_pb2.Done(ok=True)


@activity.defn(name="Ai.assessRisk")
async def assess_risk(_: systems_pb2.ClaimSnapshot) -> systems_pb2.RiskAssessment:
    return RISK


async def run_triage(env: WorkflowEnvironment, decide=None) -> tracks_pb2.TrackResult:
    workers = [
        Worker(env.client, task_queue=names.TSA_QUEUE, workflows=[TriageWorkflow],
               activities=[assess_risk, TriageRules("/app/config/triage-rules.yaml").apply_rules]),
        Worker(env.client, task_queue=names.LIFECYCLE_QUEUE, activities=[load_policy, record]),
        Worker(env.client, task_queue=names.CLAIMSPRO_QUEUE, activities=[read_claim, create_task, close_task]),
    ]
    async with workers[0], workers[1], workers[2]:
        handle = await env.client.start_workflow(
            TriageWorkflow.run, tracks_pb2.TrackRequest(claim_id="C1", stage_version=1),
            id=f"claim:C1:tsa:{uuid.uuid4()}", task_queue=names.TSA_QUEUE)
        if decide:
            await decide(handle)
        return await handle.result()


async def test_straight_through_route():
    global RISK
    RISK = systems_pb2.RiskAssessment(fraud_score=0.1, confidence=0.95)
    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await run_triage(env)
    assert result.outcome == tracks_pb2.ROUTE_STP


async def test_low_confidence_goes_to_a_triage_adjuster():
    global RISK
    RISK = systems_pb2.RiskAssessment(fraud_score=0.1, confidence=0.3)
    TASKS.clear()

    async def decide(handle):
        import asyncio
        while not TASKS:
            await asyncio.sleep(0.05)
        decision_id = next(iter(TASKS))
        with pytest.raises(WorkflowUpdateFailedError):
            await handle.execute_update("decide", decisions_pb2.Decision(
                decision_id=decision_id, choice="SIU", actor_id="x", actor_roles=["viewer"]))
        await handle.execute_update("decide", decisions_pb2.Decision(
            decision_id=decision_id, choice="SIU", actor_id="tri-1", actor_roles=["triage-adjuster"]),
            result_type=decisions_pb2.DecisionResult)

    async with await WorkflowEnvironment.start_time_skipping() as env:
        result = await run_triage(env, decide)
    assert result.outcome == tracks_pb2.ROUTE_SIU
