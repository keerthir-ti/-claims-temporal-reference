"""Activity contract for the Python tracks: activity NAMES + input/output types (types.py).

Workflows call activities by these names, so a real implementation only has to register functions
with the same names and types; workflows and workers do not change.

    loadClaim                 (claim_id) -> ClaimSnapshot          ClaimsPro          mock/
    tsa.assessRisk            (claim_id) -> RiskAssessment         triage AI          mock/
    tsa.applyTriageRules      (TriageInput) -> TriageDecision      rules engine       triage.py (real)
    is.analyzeCoverage        (claim_id) -> CoverageAnalysis       coverage AI        mock/
    is.requestDocuments       (StepRef) -> None                    ImageRight         mock/
    is.documentsReceived      (claim_id) -> int                    ImageRight         mock/
    is.recordCoverageOutcome  (StepRef, status) -> None            ClaimsPro          mock/
    recordProjection          (ProjectionRecord) -> None           history DB         Java worker (real)
"""
