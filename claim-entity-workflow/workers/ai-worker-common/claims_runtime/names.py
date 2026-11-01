"""Task queues, deployments, workflow types and message names. Same values as Names.java."""

LIFECYCLE_QUEUE = "claims-lifecycle"
TSA_QUEUE = "claims-tsa"
IS_QUEUE = "claims-is"
STP_QUEUE = "claims-stp"
CLAIMSPRO_QUEUE = "claimspro-activities"

TSA_DEPLOYMENT = "claims-tsa"
IS_DEPLOYMENT = "claims-is"

CLAIM = "ClaimWorkflow"

SIGNAL_CLAIM_EVENT = "claimEvent"
SIGNAL_CANCEL_PAYMENT = "cancelPayment"
UPDATE_DECIDE = "decide"


def claim_workflow_id(claim_id: str) -> str:
    return f"claim:{claim_id}"
