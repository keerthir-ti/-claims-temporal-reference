"""tsa.applyTriageRules (REAL, keep): evaluates config/triage-rules.yaml, first match wins."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from temporalio import activity

from claims.temporal.activities.types import ClaimSnapshot, RiskAssessment, TriageDecision, TriageInput

DEFAULT_RULES = Path(__file__).resolve().parents[4] / "config" / "triage-rules.yaml"


def _holds(cond: str, expected: Any, claim: ClaimSnapshot, risk: RiskAssessment) -> bool:
    if cond == "fraudScoreGte":
        return risk.fraudScore >= expected
    if cond == "complexityGte":
        return risk.complexity >= expected
    if cond == "coverageStatusIn":
        return claim.coverageStatus in expected
    if cond == "injuryReported":
        return claim.injuryReported == expected
    if cond == "policyActive":
        return claim.policyActive == expected
    if cond == "lobIn":
        return claim.lob in expected
    if cond == "stateNotIn":
        return claim.state not in expected
    if cond == "routingChannelIn":
        return claim.routingChannel in expected
    raise ValueError(f"unknown rule condition {cond!r}")


def evaluate(rules_doc: dict, claim: ClaimSnapshot, risk: RiskAssessment) -> TriageDecision:
    for rule in rules_doc["rules"]:
        when = rule.get("when") or {}
        if all(_holds(c, v, claim, risk) for c, v in when.items()):
            reason = ", ".join(f"{c}={v}" for c, v in when.items()) or "default"
            return TriageDecision(route=rule["route"], ruleId=rule["id"], reason=reason,
                                  rulesVersion=rules_doc.get("version", 0))
    raise ValueError("rules must end with a catch-all rule")


class TriageActivities:
    def __init__(self, rules_path: str | None = None):
        self._rules_path = Path(rules_path or os.environ.get("TRIAGE_RULES", DEFAULT_RULES))

    @activity.defn(name="tsa.applyTriageRules")
    async def apply_triage_rules(self, data: TriageInput) -> TriageDecision:
        rules = yaml.safe_load(self._rules_path.read_text())
        decision = evaluate(rules, data.claim, data.risk)
        activity.logger.info("Triage %s -> %s via %s (rules v%s)", data.claim.claimId, decision.route,
                             decision.ruleId, decision.rulesVersion)
        return decision
