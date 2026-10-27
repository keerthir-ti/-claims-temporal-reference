"""Helpers used inside Python workflow code (deterministic only)."""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from claims.temporal import names
    from claims.temporal.activities.errors import BUSINESS_RULE, NOT_FOUND, STALE_STAGE
    from claims.contracts.models import ProjectionRecord, Track

ACTIVITY_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    non_retryable_error_types=[STALE_STAGE, NOT_FOUND, BUSINESS_RULE],
)
ACTIVITY_TIMEOUT = timedelta(seconds=30)

# AI calls cost money and an outage should not be retried forever: after 5 attempts the track
# fails and the parent routes the claim to MANUAL (a human picks it up).
AI_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=5,
    non_retryable_error_types=[STALE_STAGE, NOT_FOUND, BUSINESS_RULE],
)
AI_TIMEOUT = timedelta(seconds=60)


async def record_projection(claim_id: str, stage_version: int, track: Track, step: str,
                            status: str, detail: str) -> None:
    """Calls the Java projection activity on its own task queue: a cross-language activity call."""
    info = workflow.info()
    await workflow.execute_activity(
        names.ACTIVITY_RECORD_PROJECTION,
        ProjectionRecord(
            schemaVersion=1, claimId=claim_id, stageVersion=stage_version, track=track, step=step,
            status=status, detail=detail, workflowId=info.workflow_id, runId=info.run_id,
            occurredAt=workflow.now().isoformat().replace("+00:00", "Z"),
        ),
        task_queue=names.PROJECTION_QUEUE,
        start_to_close_timeout=ACTIVITY_TIMEOUT,
        retry_policy=ACTIVITY_RETRY,
    )
