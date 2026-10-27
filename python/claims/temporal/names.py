"""Stable cross-language names. Must match java/.../common/Names.java exactly."""

LIFECYCLE_QUEUE = "claims-lifecycle"
STP_QUEUE = "claims-stp"
TSA_QUEUE = "claims-tsa"
IS_QUEUE = "claims-is"
PROJECTION_QUEUE = "claims-projection"

LIFECYCLE_WORKFLOW = "ClaimLifecycleWorkflow"
STP_WORKFLOW = "StpWorkflow"
TSA_WORKFLOW = "TsaWorkflow"
IS_WORKFLOW = "InvestigationWorkflow"

SIGNAL_CLAIM_EVENT = "claimEvent"
UPDATE_ADJUSTER_DECISION = "submitAdjusterDecision"
QUERY_STATE = "getState"

ACTIVITY_RECORD_PROJECTION = "recordProjection"


def lifecycle_workflow_id(claim_id: str) -> str:
    return f"claim:{claim_id}"

# Worker Deployment names (Worker Versioning). Java: claims-lifecycle, claims-stp.
TSA_DEPLOYMENT = "claims-tsa"
IS_DEPLOYMENT = "claims-is"
