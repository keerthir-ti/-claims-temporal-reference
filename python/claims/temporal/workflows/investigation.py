from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from claims.temporal import names
    from claims.temporal.workflows.support import (
        ACTIVITY_RETRY, ACTIVITY_TIMEOUT, AI_RETRY, AI_TIMEOUT, record_projection,
    )
    from claims.contracts.models import (
        ClaimEvent, ClaimEventType, LifecycleSettings, Track, TrackOutcome, TrackRequest, TrackResult,
    )
    from claims.temporal.activities.types import CoverageAnalysis, StepRef


@workflow.defn(name=names.IS_WORKFLOW)
class InvestigationWorkflow:
    """Investigation Support track (Python): resolve coverage uncertainty, then hand back to the
    parent, which re-triages with the updated ClaimsPro data."""

    def __init__(self) -> None:
        self._seen: set[str] = set()
        self._document_event = False

    @workflow.signal(name=names.SIGNAL_CLAIM_EVENT)
    def claim_event(self, event: ClaimEvent) -> None:
        if event.eventId in self._seen:
            return
        self._seen.add(event.eventId)
        if event.eventType == ClaimEventType.DOCUMENT_RECEIVED:
            self._document_event = True

    @workflow.run
    async def run(self, req: TrackRequest) -> TrackResult:
        settings = req.settings or LifecycleSettings()
        for event in req.bufferedEvents or []:
            self.claim_event(event)

        analysis: CoverageAnalysis = await workflow.execute_activity(
            "is.analyzeCoverage", req.claimId, result_type=CoverageAnalysis,
            start_to_close_timeout=AI_TIMEOUT, retry_policy=AI_RETRY)

        documents_ok = True
        if analysis.needsDocuments:
            await workflow.execute_activity(
                "is.requestDocuments", StepRef(claimId=req.claimId, stageVersion=req.stageVersion, step="REQUEST_DOCUMENTS"),
                start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=ACTIVITY_RETRY)
            documents_ok = await self._await_documents(req.claimId, settings)

        status = "CONFIRMED" if documents_ok and analysis.likelyCovered else "INCONCLUSIVE"
        await workflow.execute_activity(
            "is.recordCoverageOutcome",
            args=[StepRef(claimId=req.claimId, stageVersion=req.stageVersion, step="COVERAGE_OUTCOME"), status],
            start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=ACTIVITY_RETRY)
        await record_projection(req.claimId, req.stageVersion, Track.IS, "COVERAGE_OUTCOME", status,
                                f"documents={'received' if documents_ok else 'missing'}")
        return TrackResult(
            schemaVersion=1, claimId=req.claimId, track=Track.IS, stageVersion=req.stageVersion,
            outcome=TrackOutcome.COMPLETED, reason=f"coverage {status}", completedSteps=["COVERAGE_ANALYSIS"],
        )

    async def _await_documents(self, claim_id: str, settings: LifecycleSettings) -> bool:
        """Durable wait for DOCUMENT_RECEIVED with a periodic re-read of ClaimsPro as backstop."""
        deadline = workflow.now() + timedelta(seconds=settings.eventWaitSeconds or 60)
        poll = timedelta(seconds=settings.revalidateIntervalSeconds or 15)
        while True:
            remaining = deadline - workflow.now()
            if remaining <= timedelta(0):
                workflow.logger.info("Documents not received before deadline")
                return False
            try:
                await workflow.wait_condition(lambda: self._document_event, timeout=min(poll, remaining))
            except asyncio.TimeoutError:
                pass
            received = await workflow.execute_activity(
                "is.documentsReceived", claim_id, result_type=int,
                start_to_close_timeout=ACTIVITY_TIMEOUT, retry_policy=ACTIVITY_RETRY)
            if received > 0:
                if not self._document_event:
                    workflow.logger.warning("Backstop poll found documents; event was missed")
                return True
