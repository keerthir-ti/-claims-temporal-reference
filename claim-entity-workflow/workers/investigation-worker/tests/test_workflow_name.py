"""The activity name ClaimWorkflow (Java) calls on the claims-is queue."""
from temporalio import activity

from investigation_worker.mock_ai import MockCoverageAnalyst


def test_activity_name_matches_the_claim_workflow():
    assert activity._Definition.from_callable(MockCoverageAnalyst().analyze_coverage).name == "Ai.analyzeCoverage"
