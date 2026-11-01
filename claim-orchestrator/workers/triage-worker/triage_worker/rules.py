"""Triage rules from config/triage-rules.yaml. Real implementation: keep it (replace only the rules file)."""
import os

import yaml
from temporalio import activity

from claims import systems_pb2


class TriageRules:
    def __init__(self, path: str | None = None) -> None:
        with open(path or os.environ.get("TRIAGE_RULES", "config/triage-rules.yaml")) as f:
            doc = yaml.safe_load(f)
        self._rules = doc["rules"]
        self._default = doc["default"]

    @activity.defn(name="Triage.applyRules")
    async def apply_rules(self, data: systems_pb2.TriageInput) -> systems_pb2.TriageRuling:
        facts = {
            "fraud_score": data.risk.fraud_score,
            "complexity": data.risk.complexity,
            "coverage_question": data.risk.coverage_question,
            "injury_reported": data.claim.injury_reported,
        }
        route, reason = self._default["route"], self._default["reason"]
        for rule in self._rules:
            if all(_holds(cond, value, facts) for cond, value in rule["when"].items()):
                route, reason = rule["route"], rule["reason"]
                break
        return systems_pb2.TriageRuling(route=route, reason=reason,
                                        needs_review=data.risk.confidence < data.confidence_threshold)


def _holds(condition: str, value, facts: dict) -> bool:
    if condition.endswith("_at_least"):
        return facts[condition.removesuffix("_at_least")] >= value
    return facts[condition] == value
