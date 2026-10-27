"""Shared demo helpers. The demo acts as two outsiders:

  * the outside world, through the mock's demo-control API (DEMO_URL): file a claim, edit it, reopen it;
  * an operator / adjuster UI, through Temporal (Queries and Updates on the claim's workflow).

It never calls the fake external systems directly, so the same demo runs unchanged when those are
replaced by the real systems.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import httpx
from temporalio.client import Client
from temporalio.service import RPCError

from claims.temporal import names
from claims.contracts.models import AdjusterAction, AdjusterDecision, AdjusterDecisionAck, LifecycleState

DEMO_URL = os.environ.get("DEMO_URL", "http://localhost:8090/demo")
BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://localhost:8085")


async def world(method: str, path: str, body: dict | None = None) -> dict:
    async with httpx.AsyncClient(base_url=DEMO_URL, timeout=10) as http:
        resp = await http.request(method, path, json=body)
        resp.raise_for_status()
        return resp.json()


async def new_claim(fixture: str, claim_id: str | None = None) -> str:
    claim_id = claim_id or f"{fixture}-{uuid.uuid4().hex[:4].upper()}"
    await world("POST", "/claims", {"fixture": fixture, "claimId": claim_id})
    return claim_id


async def state(client: Client, claim_id: str) -> LifecycleState | None:
    try:
        return await client.get_workflow_handle(names.lifecycle_workflow_id(claim_id)).query(
            names.QUERY_STATE, result_type=LifecycleState)
    except RPCError:
        return None


async def decide(client: Client, claim_id: str, action: str, decision_id: str | None = None) -> AdjusterDecisionAck:
    handle = client.get_workflow_handle(names.lifecycle_workflow_id(claim_id))
    return await handle.execute_update(
        names.UPDATE_ADJUSTER_DECISION,
        AdjusterDecision(schemaVersion=1, decisionId=decision_id or str(uuid.uuid4()), claimId=claim_id,
                         adjusterId="adj-demo", action=AdjusterAction(action), notes="via demo"),
        result_type=AdjusterDecisionAck,
    )


async def wait_for(client: Client, claim_id: str, predicate, timeout: float = 120, label: str = "") -> LifecycleState:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        s = await state(client, claim_id)
        if s is not None:
            last = s
            if predicate(s):
                return s
        await asyncio.sleep(1)
    raise TimeoutError(f"{claim_id}: timed out waiting for {label}; last state={last and last.currentTrack}")


def in_track(*tracks: str):
    return lambda s: s.currentTrack.value in tracks


def stp_running(s: LifecycleState) -> bool:
    return s.currentTrack.value == "STP" and bool(s.activeChildWorkflowId)


def print_state(s: LifecycleState) -> None:
    print(f"\n{s.claimId}  track={s.currentTrack.value}  stageVersion={s.stageVersion}  "
          f"retriage={s.retriageCount}  activeChild={s.activeChildWorkflowId}  buffered={s.bufferedEventCount}")
    for t in s.transitions or []:
        print(f"   v{t.stageVersion:<3} {t.from_.value:<10} -> {t.to.value:<10} {t.reason}")
