"""The investigation workflow's contract: names and types the Java parent relies on."""
from claims_runtime import names
from investigation_worker.workflow import InvestigationWorkflow
from temporalio import workflow


def test_workflow_name_matches_the_parent():
    assert workflow._Definition.from_class(InvestigationWorkflow).name == names.INVESTIGATION == "InvestigationWorkflow"
