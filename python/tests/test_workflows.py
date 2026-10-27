"""Workflow tests on the time-skipping test server. Activities are stubbed by registering functions
under the same activity names, including a stand-in for the Java recordProjection activity: that is
how a Python track is tested in isolation from the Java workers it calls."""
import asyncio
import uuid

import pytest
from temporalio import activity
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from claims.temporal import names
from claims.temporal.activities.types import ClaimSnapshot
from claims.contracts.models import (
    ClaimEvent, ClaimEventType, LifecycleSettings, ProjectionRecord, Track, TrackOutcome, TrackRequest, TrackResult,
)
from claims.temporal.activities.types import CoverageAnalysis, StepRef
from claims.temporal.workflows.investigation import InvestigationWorkflow
from claims.temporal.activities.types import TriageInput
from claims.temporal.activities.types import RiskAssessment, TriageDecision
from claims.temporal.workflows.tsa import TsaWorkflow


@pytest.fixture
async def env():
    async with await WorkflowEnvironment.start_time_skipping(data_converter=pydantic_data_converter) as e:
        yield e


projections: list[ProjectionRecord] = []


@activity.defn(name=names.ACTIVITY_RECORD_PROJECTION)
async def record_projection(record: ProjectionRecord) -> None:
    projections.append(record)


def request(track: Track, **settings) -> TrackRequest:
    return TrackRequest(schemaVersion=1, claimId="C-1", track=track, stageVersion=2,
                        settings=LifecycleSettings(**settings))


async def run_with(env, workflow_cls, queue, activities, req, during=None):
    wid = f"claim:C-1:test:{uuid.uuid4()}"
    async with Worker(env.client, task_queue=queue, workflows=[workflow_cls], activities=activities), \
            Worker(env.client, task_queue=names.PROJECTION_QUEUE, activities=[record_projection]):
        handle = await env.client.start_workflow(workflow_cls.run, req, id=wid, task_queue=queue)
        if during:
            await during(handle)
        return await handle.result()


# ------------------------------------------------------------------------------------------ TSA

async def test_tsa_routes_and_reevaluates_when_claim_changes_mid_triage(env):
    version = {"n": 1}
    risk_called = asyncio.Event()
    release = asyncio.Event()

    @activity.defn(name="loadClaim")
    async def load_claim(claim_id: str) -> ClaimSnapshot:
        return ClaimSnapshot(claimId=claim_id, version=version["n"], lob="AUTO", injuryReported=version["n"] > 1)

    @activity.defn(name="tsa.assessRisk")
    async def assess_risk(claim_id: str) -> RiskAssessment:
        risk_called.set()
        await release.wait()
        return RiskAssessment(fraudScore=0.1, complexity=0.1)

    @activity.defn(name="tsa.applyTriageRules")
    async def apply_rules(data: TriageInput) -> TriageDecision:
        route = "MANUAL" if data.claim.injuryReported else "STP"
        return TriageDecision(route=route, ruleId="test", reason="", rulesVersion=1)

    async def during(handle):
        await risk_called.wait()
        version["n"] = 2  # ClaimsPro changed while the AI step was running
        await handle.signal(names.SIGNAL_CLAIM_EVENT, ClaimEvent(
            schemaVersion=1, eventId="e1", claimId="C-1", eventType=ClaimEventType.CLAIM_UPDATED))
        release.set()

    result: TrackResult = await run_with(env, TsaWorkflow, names.TSA_QUEUE, [load_claim, assess_risk, apply_rules],
                                         request(Track.TSA), during)
    assert result.outcome == TrackOutcome.ROUTE
    assert result.nextTrack == Track.MANUAL  # second pass saw the injury
    assert result.stageVersion == 2


# ------------------------------------------------------------------------------------------ IS

def is_activities(documents_received):
    @activity.defn(name="is.analyzeCoverage")
    async def analyze(claim_id: str) -> CoverageAnalysis:
        return CoverageAnalysis(needsDocuments=True, likelyCovered=True)

    @activity.defn(name="is.requestDocuments")
    async def request_docs(ref: StepRef) -> None:
        return None

    @activity.defn(name="is.documentsReceived")
    async def docs(claim_id: str) -> int:
        return documents_received()

    outcomes = []

    @activity.defn(name="is.recordCoverageOutcome")
    async def record(ref: StepRef, status: str) -> None:
        outcomes.append(status)

    return [analyze, request_docs, docs, record], outcomes


async def test_is_backstop_poll_finds_documents_when_event_is_missed(env):
    polls = {"n": 0}

    def received():
        polls["n"] += 1
        return 1 if polls["n"] >= 3 else 0  # documents appear on the 3rd poll; no event ever sent

    acts, outcomes = is_activities(received)
    result = await run_with(env, InvestigationWorkflow, names.IS_QUEUE, acts,
                            request(Track.IS, eventWaitSeconds=7 * 24 * 3600, revalidateIntervalSeconds=3600))
    assert result.outcome == TrackOutcome.COMPLETED
    assert outcomes == ["CONFIRMED"]


async def test_is_times_out_to_inconclusive(env):
    acts, outcomes = is_activities(lambda: 0)
    result = await run_with(env, InvestigationWorkflow, names.IS_QUEUE, acts,
                            request(Track.IS, eventWaitSeconds=3 * 24 * 3600, revalidateIntervalSeconds=12 * 3600))
    assert result.reason == "coverage INCONCLUSIVE"
    assert outcomes == ["INCONCLUSIVE"]


async def test_tsa_gives_up_on_an_unavailable_ai_model_after_five_attempts(env):
    """AI_RETRY caps the AI call: the track fails and the parent routes the claim to MANUAL,
    instead of retrying a paid model forever."""
    attempts = {"n": 0}

    @activity.defn(name="loadClaim")
    async def load_claim(claim_id: str) -> ClaimSnapshot:
        return ClaimSnapshot(claimId=claim_id, version=1, lob="AUTO")

    @activity.defn(name="tsa.assessRisk")
    async def assess_risk(claim_id: str) -> RiskAssessment:
        attempts["n"] += 1
        raise RuntimeError("model endpoint unavailable")

    @activity.defn(name="tsa.applyTriageRules")
    async def apply_rules(data: TriageInput) -> TriageDecision:
        raise AssertionError("must not be reached")

    from temporalio.client import WorkflowFailureError
    with pytest.raises(WorkflowFailureError):
        await run_with(env, TsaWorkflow, names.TSA_QUEUE, [load_claim, assess_risk, apply_rules],
                       request(Track.TSA))
    assert attempts["n"] == 5
