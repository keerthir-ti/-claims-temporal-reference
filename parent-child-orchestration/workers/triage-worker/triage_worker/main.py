"""Triage worker: python -m triage_worker.main

Replacing mocks: register the real AI activity instead of MockRiskAssessor, under the same name.
"""
import asyncio
import logging

from temporalio.worker import Worker

from claims_runtime import logs, names
from claims_runtime.connection import connect
from claims_runtime.versioning import pinned
from triage_worker.mock_ai import MockRiskAssessor
from triage_worker.rules import TriageRules
from triage_worker.workflow import TriageWorkflow


async def main() -> None:
    logs.configure("triage-worker")
    client = await connect(metrics=True)
    worker = Worker(
        client,
        task_queue=names.TSA_QUEUE,
        workflows=[TriageWorkflow],
        activities=[MockRiskAssessor().assess_risk, TriageRules().apply_rules],
        deployment_config=pinned(names.TSA_DEPLOYMENT),
    )
    logging.getLogger(__name__).info("Triage worker polling %s", names.TSA_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
