"""Estimate Temporal Cloud Actions for one claim from its real Event Histories (parent, children,
grandchildren, every Continue-As-New run).

    python -m demo.actions CLM-1001-ABCD [CLM-1002-ABCD ...]

Counting rules follow Temporal Cloud's published Action definitions at the time of writing; check
https://docs.temporal.io/cloud/actions before using the numbers for budgeting. Queries, Describe
and list/count visibility calls are not in history and are reported separately by you, not here.
"""
from __future__ import annotations

import asyncio
import collections
import sys

from temporalio.api.enums.v1 import EventType

from claims.temporal.client.connection import connect

BILLABLE = {
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED: "workflow / child / continue-as-new run started",
    EventType.EVENT_TYPE_TIMER_STARTED: "timer started",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED: "signal received",
    EventType.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES: "search attribute upsert",
    EventType.EVENT_TYPE_MARKER_RECORDED: "marker (version / side effect / local activity)",
    EventType.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED: "update accepted",
}


async def count(client, claim_id: str) -> collections.Counter:
    totals: collections.Counter = collections.Counter()
    query = f'WorkflowId STARTS_WITH "claim:{claim_id}"'
    async for execution in client.list_workflows(query):
        totals["executions"] += 1
        history = await client.get_workflow_handle(execution.id, run_id=execution.run_id).fetch_history()
        for event in history.events:
            if event.event_type in BILLABLE:
                totals[BILLABLE[event.event_type]] += 1
            elif event.event_type == EventType.EVENT_TYPE_ACTIVITY_TASK_STARTED:
                # Each attempt is an Action; history keeps only the last attempt's STARTED event.
                totals["activity attempts"] += event.activity_task_started_event_attributes.attempt
    return totals


async def main(claim_ids: list[str]) -> None:
    client = await connect()
    for claim_id in claim_ids:
        totals = await count(client, claim_id)
        actions = sum(v for k, v in totals.items() if k != "executions")
        print(f"\n{claim_id}: ~{actions} actions across {totals['executions']} executions")
        for k, v in sorted(totals.items(), key=lambda kv: -kv[1]):
            if k != "executions":
                print(f"   {v:5d}  {k}")


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1:]))
