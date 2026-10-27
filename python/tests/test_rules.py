from pathlib import Path

import pytest
import yaml

from claims.temporal.activities.types import ClaimSnapshot
from claims.temporal.activities.triage import RiskAssessment, evaluate

RULES = yaml.safe_load((Path(__file__).resolve().parents[2] / "config" / "triage-rules.yaml").read_text())


def claim(**kw) -> ClaimSnapshot:
    base = dict(claimId="C", version=1, lob="AUTO", state="MA", coverageStatus="CONFIRMED", policyActive=True)
    return ClaimSnapshot(**{**base, **kw})


@pytest.mark.parametrize("snapshot,risk,route,rule", [
    (claim(), RiskAssessment(fraudScore=0.1, complexity=0.2), "STP", "stp-eligible"),
    (claim(), RiskAssessment(fraudScore=0.95, complexity=0.2), "SIU", "siu-fraud"),
    (claim(coverageStatus="UNCERTAIN"), RiskAssessment(fraudScore=0.1, complexity=0.2), "IS", "coverage-uncertain"),
    (claim(coverageStatus="INCONCLUSIVE"), RiskAssessment(fraudScore=0.1, complexity=0.2), "MANUAL", "coverage-denied-or-inconclusive"),
    (claim(injuryReported=True), RiskAssessment(fraudScore=0.1, complexity=0.2), "MANUAL", "injury-needs-adjuster"),
    (claim(state="NY"), RiskAssessment(fraudScore=0.1, complexity=0.2), "MANUAL", "default-manual"),
])
def test_rules(snapshot, risk, route, rule):
    decision = evaluate(RULES, snapshot, risk)
    assert (decision.route, decision.ruleId) == (route, rule)
