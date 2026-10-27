"""Activity inputs and outputs (Python side). Activity-internal: the cross-language contracts live
in claims.contracts (generated from contracts/schema)."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class ClaimSnapshot(BaseModel):
    """ClaimsPro's view of a claim, as returned by loadClaim."""
    model_config = ConfigDict(extra="ignore")

    claimId: str
    version: int
    lob: str | None = None
    state: str | None = None
    company: str | None = None
    routingChannel: str | None = None
    lossType: str | None = None
    injuryReported: bool = False
    estimatedLossCents: int = 0
    policyActive: bool = True
    coverageStatus: str | None = None
    status: str | None = None


class RiskAssessment(BaseModel):
    """tsa.assessRisk result (triage AI)."""
    fraudScore: float
    complexity: float
    model: str = ""


class TriageInput(BaseModel):
    claim: ClaimSnapshot
    risk: RiskAssessment


class TriageDecision(BaseModel):
    """tsa.applyTriageRules result: a RECOMMENDED route; the Java parent validates it."""
    route: str
    ruleId: str
    reason: str
    rulesVersion: int


class CoverageAnalysis(BaseModel):
    """is.analyzeCoverage result (coverage-analysis AI)."""
    needsDocuments: bool
    likelyCovered: bool
    model: str = ""


class StepRef(BaseModel):
    """Input of every write activity: the tokens the target system needs (stage fence + idempotency key)."""
    claimId: str
    stageVersion: int
    step: str

    @property
    def key(self) -> str:
        return f"{self.claimId}:{self.stageVersion}:{self.step}"
