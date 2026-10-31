"""InvestigationWorkflow: gathers missing coverage facts (claim:{claimId}:is:{stage}), then asks for re-triage."""
import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claims.v1 import decisions_pb2, events_pb2, systems_pb2, tracks_pb2
    from claims_runtime import names
    from claims_runtime.workflow_support import (DecisionDesk, Recorder, Stopped, claimspro, is_business,
                                                 load_policy, retry_policy, retry_window, run_step)
    from temporalio.exceptions import ActivityError, CancelledError


@workflow.defn(name=names.INVESTIGATION)
class InvestigationWorkflow:
    @workflow.init
    def __init__(self, request: tracks_pb2.TrackRequest) -> None:
        self._req = request
        self._recorder = Recorder(request.claim_id, names.INVESTIGATION)
        self._desk = DecisionDesk(request.claim_id, request.stage_version, self._recorder)
        self._documents = any(e.type == events_pb2.DOCUMENTS_RECEIVED for e in request.buffered_events)

    @workflow.run
    async def run(self, request: tracks_pb2.TrackRequest) -> tracks_pb2.TrackResult:
        try:
            return await self._investigate()
        except asyncio.CancelledError:
            await self._cancelled()
            raise
        except Stopped as stopped:
            return await self._finish(tracks_pb2.ROUTE_MANUAL, f"stopped at {stopped}")
        except ActivityError as e:
            if isinstance(e.cause, CancelledError):
                await self._cancelled()
                raise
            if is_business(e):
                return await self._finish(tracks_pb2.ROUTE_MANUAL, f"{e.cause}")
            raise

    async def _cancelled(self) -> None:
        """Cancelled by the parent: withdraw open tasks and write the closing row."""
        await self._desk.withdraw_all()
        await self._recorder.closing(self._req.stage_version, "CANCELLED")

    async def _investigate(self) -> tracks_pb2.TrackResult:
        v = self._req.stage_version
        policy = await load_policy(self._req.claim_id)
        self._desk.policy = policy
        window = retry_window(policy)
        ref = systems_pb2.ClaimRef(claim_id=self._req.claim_id)
        attempt = 0
        while True:
            attempt += 1
            self._documents = False
            claim = await run_step(self._desk, "is.read",
                                   lambda: claimspro("readClaim", ref, systems_pb2.ClaimSnapshot, window))
            analysis = await run_step(self._desk, "is.coverage", lambda: workflow.execute_activity(
                "Ai.analyzeCoverage", claim, start_to_close_timeout=timedelta(seconds=60),
                retry_policy=retry_policy(), result_type=systems_pb2.CoverageAnalysis))
            await self._recorder.row(v, f"is.analysis.{attempt}",
                                     outcome="COMPLETE" if analysis.information_complete else "MISSING",
                                     reason=analysis.missing)
            if analysis.information_complete:
                return await self._finish(tracks_pb2.RETRIAGE, "coverage facts gathered")
            # Documents arriving in ImageRight (an event) or an IS reviewer's decision end the wait.
            d = await self._desk.decide(decisions_pb2.COVERAGE_INFORMATION, ["INFORMATION_RECEIVED", "MANUAL"],
                                        analysis.missing, interrupt=lambda: self._documents)
            if d is None:
                continue  # documents received: analyse again
            if d.choice == "INFORMATION_RECEIVED":
                return await self._finish(tracks_pb2.RETRIAGE, f"information confirmed by {d.decided_by}")
            return await self._finish(tracks_pb2.ROUTE_MANUAL, "coverage information not obtained")

    async def _finish(self, outcome: int, reason: str) -> tracks_pb2.TrackResult:
        await self._recorder.closing(self._req.stage_version, tracks_pb2.TrackOutcome.Name(outcome))
        return tracks_pb2.TrackResult(outcome=outcome, reason=reason)

    @workflow.signal(name=names.SIGNAL_CLAIM_EVENT)
    def claim_event(self, event: events_pb2.ClaimEvent) -> None:
        if event.type in (events_pb2.DOCUMENTS_RECEIVED, events_pb2.CLAIM_UPDATED):
            self._documents = True

    @workflow.update(name=names.UPDATE_DECIDE)
    def decide(self, decision: decisions_pb2.Decision) -> decisions_pb2.DecisionResult:
        return self._desk.handle(decision)

    @decide.validator
    def validate_decide(self, decision: decisions_pb2.Decision) -> None:
        self._desk.validate(decision)
