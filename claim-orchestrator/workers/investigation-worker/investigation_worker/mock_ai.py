"""MOCK of the coverage AI agent. Coverage facts are missing while the scenario says so and no documents
have arrived. Replace with the real agent call under the same activity name ("Ai.analyzeCoverage")."""
import json
import os

import psycopg
from temporalio import activity

from claims import systems_pb2


class MockCoverageAnalyst:
    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn or os.environ.get("MOCK_DSN", "postgresql://claims:claims@localhost:5432/claims")

    @activity.defn(name="Ai.analyzeCoverage")
    async def analyze_coverage(self, claim: systems_pb2.ClaimSnapshot) -> systems_pb2.CoverageAnalysis:
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            row = await (await conn.execute(
                "SELECT behaviour, documents_received FROM mock.claims WHERE claim_id = %s", (claim.claim_id,)
            )).fetchone()
        behaviour = (row[0] if isinstance(row[0], dict) else json.loads(row[0] or "{}")) if row else {}
        missing = bool(behaviour.get("ai", {}).get("coverage_question", False)) and not (row and row[1])
        return systems_pb2.CoverageAnalysis(
            information_complete=not missing,
            missing="proof of ownership and police report" if missing else "",
            confidence=0.9,
        )
