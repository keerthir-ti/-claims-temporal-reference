import asyncio

from claims import systems_pb2
from triage_worker.rules import TriageRules

RULES = TriageRules("/app/config/triage-rules.yaml")


def ruling(**risk) -> systems_pb2.TriageRuling:
    return asyncio.run(_ruling(**risk))


async def _ruling(**risk) -> systems_pb2.TriageRuling:
    injury = risk.pop("injury", False)
    return await RULES.apply_rules(systems_pb2.TriageInput(
        claim=systems_pb2.ClaimSnapshot(claim_id="C1", injury_reported=injury),
        risk=systems_pb2.RiskAssessment(confidence=risk.pop("confidence", 0.9), **risk),
        confidence_threshold=0.6))


def test_routes_first_matching_rule():
    assert ruling(fraud_score=0.9, injury=True).route == "SIU"
    assert ruling(injury=True).route == "MANUAL"
    assert ruling(coverage_question=True).route == "INVESTIGATION"
    assert ruling(complexity=0.8).route == "MANUAL"
    assert ruling().route == "STP"


def test_low_confidence_needs_review():
    assert ruling(confidence=0.4).needs_review
    assert not ruling(confidence=0.9).needs_review
