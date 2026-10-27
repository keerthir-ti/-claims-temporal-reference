"""MOCK of the IS (investigative support) activities."""
from __future__ import annotations

from temporalio import activity

from claims.temporal.activities.mock.systems_client import MockSystemsClient
from claims.temporal.activities.mock.tsa import MockTsaActivities
from claims.temporal.activities.types import CoverageAnalysis, StepRef


class MockInvestigationActivities:
    def __init__(self, systems: MockSystemsClient):
        self._systems = systems
        self.load_claim = MockTsaActivities(systems).load_claim  # same ClaimsPro read as TSA

    @activity.defn(name="is.analyzeCoverage")
    async def analyze_coverage(self, claim_id: str) -> CoverageAnalysis:
        """Coverage-analysis AI (document / LLM agent)."""
        return CoverageAnalysis.model_validate(await self._systems.post("/ai/coverage/analyze", {"claimId": claim_id}))

    @activity.defn(name="is.requestDocuments")
    async def request_documents(self, ref: StepRef) -> None:
        """ImageRight: request documents (they arrive later as an event)."""
        await self._systems.post(f"/imageright/claims/{ref.claimId}/document-requests", {}, ref)

    @activity.defn(name="is.documentsReceived")
    async def documents_received(self, claim_id: str) -> int:
        """ImageRight: backstop poll when the DOCUMENT_RECEIVED event may have been missed."""
        return int((await self._systems.get(f"/imageright/claims/{claim_id}/documents"))["received"])

    @activity.defn(name="is.recordCoverageOutcome")
    async def record_coverage_outcome(self, ref: StepRef, coverage_status: str) -> None:
        """ClaimsPro: record the coverage result (ClaimsPro then emits a change event)."""
        await self._systems.post(f"/claimspro/claims/{ref.claimId}/coverage", {"coverageStatus": coverage_status}, ref)
