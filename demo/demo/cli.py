"""Demo / operator CLI (run through `make`, inside Docker).

    python -m demo.cli q1 … q7                     # walk through our answer to one of the 7 design questions
    python -m demo.cli demo                        # run all 6 scripted claims and check every outcome
    python -m demo.cli seed CLM-1001 [--id X]      # file a claim -> FNOL event -> bridge -> workflow
    python -m demo.cli state X | progress X        # Query the Java parent / the active STP child
    python -m demo.cli patch X injuryReported=true # change the claim in ClaimsPro (emits a change event)
    python -m demo.cli decide X CLOSE              # adjuster decision: Update on the parent
    python -m demo.cli reopen X | cancel-payment X | ledger X
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import uuid

from temporalio.client import Client, WorkflowUpdateFailedError

from claims.temporal.client.connection import connect
from claims.contracts.models import AdjusterAction
from demo import questions
from demo.common import decide, in_track, print_state, state, stp_running, wait_for, world


def _value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


async def demo(client: Client) -> int:
    """Files every scripted claim and drives the interactive ones. Non-zero if an outcome is wrong."""
    results: list[tuple[str, str, bool]] = []
    # Fresh claim ids per run: re-filing an id whose lifecycle is still open (e.g. inside its reopen
    # window) is correctly treated as a duplicate FNOL by Signal-With-Start.
    run = uuid.uuid4().hex[:4].upper()
    fixtures = (await world("GET", "/fixtures"))["fixtures"]
    ids = {f["name"]: f"{f['name']}-{run}" for f in fixtures}
    for f in fixtures:
        await world("POST", "/claims", {"fixture": f["name"], "claimId": ids[f["name"]]})
        print(f"filed {ids[f['name']]}: {f['scenario']}")

    async def scenario(claim_id: str, steps) -> None:
        try:
            await steps()
            results.append((claim_id, "ok", True))
        except Exception as e:  # noqa: BLE001
            results.append((claim_id, str(e), False))

    async def happy(claim_id: str):
        await wait_for(client, claim_id, in_track("CLOSED"), label="CLOSED")

    async def clm_1001():
        await happy(ids["CLM-1001"])
        ledger = await world("GET", f"/claims/{ids['CLM-1001']}/ledger")
        assert len(ledger["payments"]) == 1, f"expected 1 payment, got {len(ledger['payments'])}"
        assert ledger["idempotentReplays"] >= 1, "expected the retried payment to be de-duplicated"

    async def clm_1002():
        await wait_for(client, ids["CLM-1002"], stp_running, label="STP running")
        await asyncio.sleep(4)  # let STP get into its estimate wait
        await world("PATCH", f"/claims/{ids['CLM-1002']}", {"injuryReported": True})
        s = await wait_for(client, ids["CLM-1002"], in_track("MANUAL"), label="MANUAL after re-triage")
        assert s.retriageCount == 1, f"expected one re-triage, got {s.retriageCount}"
        await decide(client, ids["CLM-1002"], "CLOSE")
        await wait_for(client, ids["CLM-1002"], in_track("CLOSED"), label="CLOSED")

    async def clm_1004():
        await wait_for(client, ids["CLM-1004"], in_track("SIU"), label="SIU")
        decision_id = str(uuid.uuid4())
        first = await decide(client, ids["CLM-1004"], "CLOSE", decision_id)
        again = await decide(client, ids["CLM-1004"], "CLOSE", decision_id)  # client retry: same decisionId
        assert not first.duplicate and again.duplicate, "decision Update should be idempotent"
        await wait_for(client, ids["CLM-1004"], in_track("CLOSED"), label="CLOSED")

    async def clm_1005():
        await wait_for(client, ids["CLM-1005"], in_track("MANUAL"), label="MANUAL (authority handoff)")
        try:
            await decide(client, ids["CLM-1005"], "UNKNOWN")
            raise AssertionError("validator should reject UNKNOWN action")
        except WorkflowUpdateFailedError:
            pass
        await decide(client, ids["CLM-1005"], "CLOSE")
        await wait_for(client, ids["CLM-1005"], in_track("CLOSED"), label="CLOSED")

    await asyncio.gather(
        scenario(ids["CLM-1001"], clm_1001), scenario(ids["CLM-1002"], clm_1002),
        scenario(ids["CLM-1003"], lambda: happy(ids["CLM-1003"])), scenario(ids["CLM-1004"], clm_1004),
        scenario(ids["CLM-1005"], clm_1005), scenario(ids["CLM-1006"], lambda: happy(ids["CLM-1006"])),
    )
    print("\n================ demo results ================")
    for claim_id, detail, ok in sorted(results):
        print(f"{'PASS' if ok else 'FAIL'}  {claim_id}  {detail}")
        s = await state(client, claim_id)
        if s:
            print_state(s)
    return 0 if all(ok for _, _, ok in results) else 1


async def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="demo.cli")
    sub = p.add_subparsers(dest="cmd", required=True)
    for q in questions.QUESTIONS:
        sub.add_parser(q, help=questions.QUESTIONS[q][0])
    ps = sub.add_parser("seed")
    ps.add_argument("claim_id", help="fixture name, e.g. CLM-1002")
    ps.add_argument("--id", help="claim id to create (default: the fixture name); use a fresh one per demo run")
    for cmd in ("state", "progress", "ledger", "reopen", "cancel-payment"):
        sub.add_parser(cmd).add_argument("claim_id")
    pp = sub.add_parser("patch")
    pp.add_argument("claim_id")
    pp.add_argument("fields", nargs="+", help="field=value (JSON values)")
    pd = sub.add_parser("decide")
    pd.add_argument("claim_id")
    pd.add_argument("action", choices=[a.value for a in AdjusterAction])
    pd.add_argument("--decision-id")
    sub.add_parser("demo")
    args = p.parse_args(argv)

    if args.cmd == "seed":
        body = {"fixture": args.claim_id, **({"claimId": args.id} if args.id else {})}
        print(json.dumps(await world("POST", "/claims", body), indent=2))
    elif args.cmd == "patch":
        body = {k: _value(v) for k, v in (f.split("=", 1) for f in args.fields)}
        print(json.dumps(await world("PATCH", f"/claims/{args.claim_id}", body), indent=2))
    elif args.cmd == "reopen":
        print(json.dumps(await world("POST", f"/claims/{args.claim_id}/reopen"), indent=2))
    elif args.cmd == "cancel-payment":
        print(json.dumps(await world("POST", f"/claims/{args.claim_id}/cancel-payment-request"), indent=2))
    elif args.cmd == "ledger":
        print(json.dumps(await world("GET", f"/claims/{args.claim_id}/ledger"), indent=2))
    else:
        client = await connect()
        if args.cmd in questions.QUESTIONS:
            return await questions.QUESTIONS[args.cmd][1](client)
        if args.cmd == "state":
            s = await state(client, args.claim_id)
            if s is None:
                print("no lifecycle workflow found")
                return 1
            print_state(s)
        elif args.cmd == "progress":
            s = await state(client, args.claim_id)
            if not s or not s.activeChildWorkflowId:
                print("no active child")
                return 1
            progress = await client.get_workflow_handle(s.activeChildWorkflowId).query("getProgress")
            print(json.dumps(progress, indent=2))
        elif args.cmd == "decide":
            print(await decide(client, args.claim_id, args.action, args.decision_id))
        elif args.cmd == "demo":
            return await demo(client)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main(sys.argv[1:])))
