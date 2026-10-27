"""MOCK of the TSA (triage) activities."""
from __future__ import annotations

from temporalio import activity

from claims.temporal.activities.mock.systems_client import MockSystemsClient
from claims.temporal.activities.types import ClaimSnapshot, RiskAssessment


class MockTsaActivities:
    def __init__(self, systems: MockSystemsClient):
        self._systems = systems

    @activity.defn(name="loadClaim")
    async def load_claim(self, claim_id: str) -> ClaimSnapshot:
        """ClaimsPro: the child starts from the current claim, never from data passed in."""
        return ClaimSnapshot.model_validate(await self._systems.get(f"/claimspro/claims/{claim_id}"))

    @activity.defn(name="tsa.assessRisk")
    async def assess_risk(self, claim_id: str) -> RiskAssessment:
        """Triage AI. Real version: the agent / LLM call, traced in the AI observability tool. The
        result is recorded in Event History, so a replay never calls the model again."""
        return RiskAssessment.model_validate(await self._systems.post("/ai/risk/score", {"claimId": claim_id}))
