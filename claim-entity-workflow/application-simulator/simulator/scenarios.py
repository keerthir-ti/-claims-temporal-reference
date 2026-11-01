"""Runs scenario files end to end: seeds the claim, plays its steps, and checks the outcome.

A scenario passes when the claim is CLOSED, its workflow completed, and its business record is complete
(every run has rows 1..n and a closing row).
"""
from __future__ import annotations

import asyncio
import time
from pathlib import Path

from temporalio.client import Client, WorkflowExecutionStatus

from claims_runtime import names
from simulator import world

TIMEOUT_SECONDS = 240


async def run(client: Client, db, scenarios_dir: Path, selected: list[str]) -> int:
    files = world.scenario_files(scenarios_dir)
    chosen = selected or list(files)
    results = await asyncio.gather(*(play(client, db, name, world.load(files[name])) for name in chosen),
                                   return_exceptions=True)
    failed = 0
    print()
    for name, result in zip(chosen, results):
        ok = not isinstance(result, BaseException) and result[0]
        failed += 0 if ok else 1
        detail = result[1] if not isinstance(result, BaseException) else f"error: {result!r}"
        print(f"  {'PASS' if ok else 'FAIL'}  {name:<28} {detail}")
    print()
    return 1 if failed else 0


async def play(client: Client, db, name: str, doc: dict) -> tuple[bool, str]:
    claim_id = await world.seed(db, name, doc)
    say(claim_id, f"filed ({doc['description']})")
    deadline = time.monotonic() + TIMEOUT_SECONDS
    for step in doc.get("demo", {}).get("steps", []):
        if "wait" in step:
            await wait(client, db, claim_id, step["wait"], deadline)
        elif "change" in step:
            await world.change(db, claim_id, step["change"])
            say(claim_id, f"change in the claims systems: {step['change']}")
        elif "decide" in step:
            await decide(client, db, claim_id, step["decide"])
    await wait(client, db, claim_id, {"stage": "CLOSED", "workflow": "completed"}, deadline)
    problems = await record_problems(db, claim_id)
    payments = await (await db.execute(
        "SELECT count(*) FROM mock.payments WHERE claim_id = %s AND status = 'CLOSED'", (claim_id,))).fetchone()
    if problems:
        return False, f"{claim_id}: incomplete record: {problems}"
    return True, f"{claim_id}: closed, record complete, {payments[0]} payment(s)"


async def wait(client: Client, db, claim_id: str, what: dict, deadline: float) -> None:
    while time.monotonic() < deadline:
        if "decision" in what and await world.open_task(db, claim_id, what["decision"]):
            say(claim_id, f"task open: {what['decision']}")
            return
        if "step" in what and await (await db.execute(
                "SELECT 1 FROM projection.claim_events WHERE claim_id = %s AND step = %s",
                (claim_id, what["step"]))).fetchone():
            return
        if "stage" in what:
            row = await (await db.execute("SELECT stage FROM projection.claim_stage WHERE claim_id = %s",
                                          (claim_id,))).fetchone()
            if row and row[0] == "STAGE_" + what["stage"]:
                if what.get("workflow") != "completed" or await completed(client, claim_id):
                    return
        await asyncio.sleep(0.5)
    raise TimeoutError(f"{claim_id}: timed out waiting for {what}")


async def completed(client: Client, claim_id: str) -> bool:
    desc = await client.get_workflow_handle(names.claim_workflow_id(claim_id)).describe()
    return desc.status == WorkflowExecutionStatus.COMPLETED


async def decide(client: Client, db, claim_id: str, d: dict) -> None:
    task = await world.open_task(db, claim_id, await kind_of_open_task(db, claim_id))
    _, decision_id, workflow_id, _, _ = task
    ok, detail = await world.decide(client, workflow_id, decision_id, d["choice"], d["as"], d["roles"],
                                    d.get("authority_cents", 0), d.get("reason", ""))
    expected = d.get("expect", "accepted") == "accepted"
    say(claim_id, f"{d['as']} decides {d['choice']}: {'accepted' if ok else 'rejected'} ({detail})")
    if ok != expected:
        raise AssertionError(f"{claim_id}: decision by {d['as']} was {'accepted' if ok else 'rejected'}: {detail}")


async def kind_of_open_task(db, claim_id: str) -> str:
    row = await (await db.execute(
        "SELECT kind FROM mock.tasks WHERE claim_id = %s AND status = 'OPEN' ORDER BY created_at DESC LIMIT 1",
        (claim_id,))).fetchone()
    return row[0] if row else ""


async def record_problems(db, claim_id: str) -> list[str]:
    rows = await (await db.execute(
        """SELECT workflow_type, run_id, count(*), max(seq), bool_or(closing) FROM projection.claim_events
           WHERE claim_id = %s GROUP BY workflow_type, run_id""", (claim_id,))).fetchall()
    return [f"{t} {r[:8]}: {c}/{m} rows, closed={cl}" for t, r, c, m, cl in rows if c != m or not cl]


def say(claim_id: str, text: str) -> None:
    print(f"  {claim_id:<16} {text}", flush=True)
