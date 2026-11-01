"""Investigation (IS) worker, activities only: python -m investigation_worker.main

The investigation logic runs inside ClaimWorkflow; this worker runs the coverage AI agent.

Replacing mocks: register the real AI activity instead of MockCoverageAnalyst, under the same name.
"""
import asyncio
import logging

from temporalio.worker import Worker

from claims_runtime import logs, names
from claims_runtime.connection import connect
from claims_runtime.versioning import pinned
from investigation_worker.mock_ai import MockCoverageAnalyst


async def main() -> None:
    logs.configure("investigation-worker")
    client = await connect(metrics=True)
    worker = Worker(
        client,
        task_queue=names.IS_QUEUE,
        activities=[MockCoverageAnalyst().analyze_coverage],
        deployment_config=pinned(names.IS_DEPLOYMENT),
    )
    logging.getLogger(__name__).info("Investigation worker polling %s", names.IS_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
