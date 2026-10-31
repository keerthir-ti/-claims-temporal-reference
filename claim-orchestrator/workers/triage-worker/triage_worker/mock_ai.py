"""MOCK of the triage AI agent. Returns the scenario's scripted assessment from the mock schema.

Replace with the real agent call under the same activity name ("Ai.assessRisk") and types.
"""
import json
import os

import psycopg
from temporalio import activity

from claims.v1 import systems_pb2


class MockRiskAssessor:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or os.environ.get("MOCK_DSN", "postgresql://claims:claims@localhost:5432/claims")

    @activity.defn(name="Ai.assessRisk")
    async def assess_risk(self, claim: systems_pb2.ClaimSnapshot) -> systems_pb2.RiskAssessment:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            row = await (await conn.execute(
                "SELECT behaviour, documents_received FROM mock.claims WHERE claim_id = %s", (claim.claim_id,)
            )).fetchone()
        behaviour = (row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")) if row else {}
        ai = behaviour.get("ai", {})
        documents = bool(row[1]) if row else False
        return systems_pb2.RiskAssessment(
            fraud_score=ai.get("fraud_score", min(1.0, claim.prior_claims * 0.2)),
            complexity=ai.get("complexity", 0.3),
            confidence=ai.get("confidence", 0.9),
            coverage_question=bool(ai.get("coverage_question", False)) and not documents,
            summary="scripted assessment (mock)",
        )
