"""Event bridge stand-in: delivers mock.change_events to Temporal the way the real bridge does.

Created and reopened claims use Signal-With-Start on claim:{claimId}; other events are signals. An
event is marked delivered only after Temporal accepts it; an event for a claim with no running workflow
is dead-lettered. Same behaviour as ClaimEventsClient.java, which the real (Java) bridge uses.
"""
from __future__ import annotations

import asyncio
import logging

from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.client import Client
from temporalio.common import SearchAttributeKey, SearchAttributePair, TypedSearchAttributes, WorkflowIDReusePolicy
from temporalio.service import RPCError, RPCStatusCode

from claims import events_pb2, lifecycle_pb2
from claims_runtime import names

log = logging.getLogger("bridge")
CLAIM_ID = SearchAttributeKey.for_keyword("ClaimId")
STARTS = {"CLAIM_CREATED", "CLAIM_REOPENED"}


async def publish(client: Client, event_id: str, claim_id: str, type_: str, source: str, occurred_at) -> str:
    ts = Timestamp()
    ts.FromDatetime(occurred_at)
    event = events_pb2.ClaimEvent(event_id=event_id, claim_id=claim_id, source_system=source, occurred_at=ts,
                                  type=events_pb2.ClaimEventType.Value(type_))
    workflow_id = names.claim_workflow_id(claim_id)
    if type_ in STARTS:
        await client.start_workflow(
            names.CLAIM,
            lifecycle_pb2.ClaimStart(claim_id=claim_id, reopened=type_ == "CLAIM_REOPENED", start_event_id=event_id),
            id=workflow_id, task_queue=names.LIFECYCLE_QUEUE,
            start_signal=names.SIGNAL_CLAIM_EVENT, start_signal_args=[event],
            id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
            search_attributes=TypedSearchAttributes([SearchAttributePair(CLAIM_ID, claim_id)]),
        )
        return "started or signalled"
    try:
        await client.get_workflow_handle(workflow_id).signal(names.SIGNAL_CLAIM_EVENT, event)
        return "signalled"
    except RPCError as e:
        if e.status == RPCStatusCode.NOT_FOUND:
            return "dead-letter: no running workflow"
        raise


async def run_feed(client: Client, db) -> None:
    log.info("Event feed running")
    while True:
        rows = await (await db.execute(
            """SELECT position, event_id, claim_id, type, source_system, occurred_at FROM mock.change_events
               WHERE delivered_at IS NULL AND visible_at <= now() ORDER BY visible_at, position LIMIT 100""")).fetchall()
        for position, event_id, claim_id, type_, source, occurred_at in rows:
            try:
                result = await publish(client, event_id, claim_id, type_, source, occurred_at)
            except Exception as e:  # Temporal unavailable: keep the position, try again
                log.warning("Event %s not delivered yet: %s", event_id, e)
                break
            await db.execute("UPDATE mock.change_events SET delivered_at = now(), result = %s WHERE position = %s",
                             (result, position))
            log.info("%s %s %s -> %s", claim_id, type_, event_id, result)
        await asyncio.sleep(0.5)
