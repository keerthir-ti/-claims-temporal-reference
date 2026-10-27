from __future__ import annotations

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claims.temporal import names
    from claims.temporal.activities.types import ClaimSnapshot
    from claims.temporal.workflows.support import (
        ACTIVITY_RETRY, ACTIVITY_TIMEOUT, AI_RETRY, AI_TIMEOUT, record_projection,
    )
    from claims.contracts.models import (
        ClaimEvent, ClaimEventType, Track, TrackOutcome, TrackRequest, TrackResult,
    )
    from claims.temporal.activities.types import TriageInput
    from claims.temporal.activities.types import RiskAssessment, TriageDecision

MAX_PASSES = 3


@workflow.defn(name=names.TSA_WORKFLOW)
class TsaWorkflow:
    """Triage track (Python). Recommends a route; the Java parent validates and applies it."""

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._claim_changed = False

    @workflow.signal(name=names.SIGNAL_CLAIM_EVENT)
    def claim_event(self, event: ClaimEvent) -> None:
        if event.eventId in self._seen:
            return
        self._seen.add(event.eventId)
        if event.eventType == ClaimEventType.CLAIM_UPDATED:
            self._claim_changed = True

    @workflow.run
    async def run(self, req: TrackRequest) -> TrackResult:
        decision: TriageDecision | None = None
        for attempt in range(1, MAX_PASSES + 1):
            self._claim_changed = False
            claim: ClaimSnapshot = await workflow.execute_activity(
                "loadClaim", req.claimId, result_type=ClaimSnapshot,
                start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=ACTIVITY_RETRY)
            risk: RiskAssessment = await workflow.execute_activity(
                "tsa.assessRisk", req.claimId, result_type=RiskAssessment,
                start_to_close_timeout=AI_TIMEOUT, retry_policy=AI_RETRY)
            decision = await workflow.execute_activity(
                "tsa.applyTriageRules", TriageInput(claim=claim, risk=risk), result_type=TriageDecision,
                start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=ACTIVITY_RETRY)
            if not self._claim_changed:
                break
            workflow.logger.info("Claim changed during triage pass %d; re-evaluating", attempt)

        assert decision is not None
        await record_projection(req.claimId, req.stageVersion, Track.TSA, "TRIAGE_DECISION", "OK",
                                f"{decision.route} by {decision.ruleId} (rules v{decision.rulesVersion})")
        return TrackResult(
            schemaVersion=1, claimId=req.claimId, track=Track.TSA, stageVersion=req.stageVersion,
            outcome=TrackOutcome.ROUTE, nextTrack=Track.__members__.get(decision.route, Track.UNKNOWN), ruleId=decision.ruleId,
            reason=decision.reason, completedSteps=["TRIAGE"],
        )
