"""Python worker. One process per track in production (separate deployments / task queues):

    python -m claims.temporal.workers.main tsa
    python -m claims.temporal.workers.main is

Swapping mock activities for real ones happens here: register the real implementations under the
same activity names instead of the Mock* classes.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys

from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import Worker, WorkerDeploymentConfig

from claims.temporal import names
from claims.temporal.activities.mock.investigation import MockInvestigationActivities
from claims.temporal.activities.mock.systems_client import MockSystemsClient
from claims.temporal.activities.mock.tsa import MockTsaActivities
from claims.temporal.activities.triage import TriageActivities
from claims.temporal.client import log_format
from claims.temporal.client.connection import connect
from claims.temporal.workflows.investigation import InvestigationWorkflow
from claims.temporal.workflows.tsa import TsaWorkflow


def deployment(name: str) -> WorkerDeploymentConfig | None:
    """Worker Deployment Versioning. Track children are short-lived, so they are PINNED to the code
    they started on. A new BUILD_ID only receives work once promoted (set-current-version)."""
    if os.environ.get("WORKER_VERSIONING", "true").lower() != "true":
        return None
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment_name=name, build_id=os.environ.get("BUILD_ID", "dev")),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )


def build_workers(client, tracks: list[str]) -> list[Worker]:
    systems = MockSystemsClient()
    workers = []
    if "tsa" in tracks:
        tsa = MockTsaActivities(systems)
        workers.append(Worker(client, task_queue=names.TSA_QUEUE, workflows=[TsaWorkflow],
                              activities=[tsa.load_claim, tsa.assess_risk, TriageActivities().apply_triage_rules],
                              deployment_config=deployment(names.TSA_DEPLOYMENT)))
    if "is" in tracks:
        inv = MockInvestigationActivities(systems)
        workers.append(Worker(client, task_queue=names.IS_QUEUE, workflows=[InvestigationWorkflow],
                              activities=[inv.load_claim, inv.analyze_coverage, inv.request_documents,
                                          inv.documents_received, inv.record_coverage_outcome],
                              deployment_config=deployment(names.IS_DEPLOYMENT)))
    return workers


async def main(tracks: list[str]) -> None:
    log_format.configure(f"claims-py-{'-'.join(tracks)}")
    client = await connect(metrics=True)
    workers = build_workers(client, tracks)
    logging.getLogger(__name__).info("Python workers running for %s", tracks)
    await asyncio.gather(*(w.run() for w in workers))


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:] or ["tsa", "is"]))
