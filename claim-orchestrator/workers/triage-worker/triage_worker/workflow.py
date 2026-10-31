"""TriageWorkflow: one triage visit (claim:{claimId}:tsa:{stage}). Returns a route; never moves the claim."""
import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claims.v1 import common_pb2, decisions_pb2, events_pb2, systems_pb2, tracks_pb2
    from claims_runtime import names
    from claims_runtime.workflow_support import (DEADLINE_PASSED, DecisionDesk, Recorder, Stopped, claimspro,
                                                 is_business, load_policy, retry_policy, retry_window, run_step)
    from temporalio.exceptions import ActivityError, CancelledError

ROUTES = {
    "STP": tracks_pb2.ROUTE_STP,
    "INVESTIGATION": tracks_pb2.ROUTE_INVESTIGATION,
    "MANUAL": tracks_pb2.ROUTE_MANUAL,
    "SIU": tracks_pb2.ROUTE_SIU,
}


@workflow.defn(name=names.TRIAGE)
class TriageWorkflow:
    @workflow.init
    def __init__(self, request: tracks_pb2.TrackRequest) -> None:
        self._req = request
        self._recorder = Recorder(request.claim_id, names.TRIAGE)
        self._desk = DecisionDesk(request.claim_id, request.stage_version, self._recorder)
        self._changed = False

    @workflow.run
    async def run(self, request: tracks_pb2.TrackRequest) -> tracks_pb2.TrackResult:
        try:
            return await self._triage()
        except asyncio.CancelledError:
            await self._cancelled()
            raise
        except Stopped as stopped:
            return await self._finish("MANUAL", f"stopped at {stopped}")
        except ActivityError as e:
            if isinstance(e.cause, CancelledError):
                await self._cancelled()
                raise
            if is_business(e):
                return await self._finish("MANUAL", f"{e.cause}")
            raise

    async def _cancelled(self) -> None:
        """Cancelled by the parent: withdraw open tasks and write the closing row."""
        await self._desk.withdraw_all()
        await self._recorder.closing(self._req.stage_version, "CANCELLED")

    async def _triage(self) -> tracks_pb2.TrackResult:
        v = self._req.stage_version
        policy = await load_policy(self._req.claim_id)
        self._desk.policy = policy
        window = retry_window(policy)
        ref = systems_pb2.ClaimRef(claim_id=self._req.claim_id)
        claim = await run_step(self._desk, "triage.read",
                               lambda: claimspro("readClaim", ref, systems_pb2.ClaimSnapshot, window))
        risk = await run_step(self._desk, "triage.risk", lambda: workflow.execute_activity(
            "Ai.assessRisk", claim, start_to_close_timeout=timedelta(seconds=60),
            retry_policy=_ai_retry(), result_type=systems_pb2.RiskAssessment))
        ruling = await workflow.execute_activity(
            "Triage.applyRules",
            systems_pb2.TriageInput(claim=claim, risk=risk, confidence_threshold=policy.triage_confidence_threshold),
            start_to_close_timeout=timedelta(seconds=10), retry_policy=retry_policy(),
            result_type=systems_pb2.TriageRuling)
        await self._recorder.row(v, "triage.ruling", outcome=ruling.route, reason=ruling.reason,
                                 refs={"fraud_score": f"{risk.fraud_score:.2f}", "confidence": f"{risk.confidence:.2f}"})
        route, reason = ruling.route, ruling.reason
        if ruling.needs_review:
            d = await self._desk.decide(decisions_pb2.TRIAGE_REVIEW, list(ROUTES),
                                        f"AI confidence {risk.confidence:.2f}; suggested {route}: {reason}")
            if d is None or d.choice == DEADLINE_PASSED:
                route, reason = "MANUAL", "triage review not completed"
            else:
                route, reason = d.choice, f"chosen by {d.decided_by} on triage review"
        return await self._finish(route, reason)

    async def _finish(self, route: str, reason: str) -> tracks_pb2.TrackResult:
        await self._recorder.closing(self._req.stage_version, route)
        return tracks_pb2.TrackResult(outcome=ROUTES.get(route, tracks_pb2.ROUTE_MANUAL), reason=reason)

    @workflow.signal(name=names.SIGNAL_CLAIM_EVENT)
    def claim_event(self, event: events_pb2.ClaimEvent) -> None:
        # Triage reads the claim when it runs; a change after that is picked up by the next visit.
        self._changed = True

    @workflow.update(name=names.UPDATE_DECIDE)
    def decide(self, decision: decisions_pb2.Decision) -> decisions_pb2.DecisionResult:
        return self._desk.handle(decision)

    @decide.validator
    def validate_decide(self, decision: decisions_pb2.Decision) -> None:
        self._desk.validate(decision)


def _ai_retry():
    policy = retry_policy()
    policy.maximum_attempts = 5
    return policy
