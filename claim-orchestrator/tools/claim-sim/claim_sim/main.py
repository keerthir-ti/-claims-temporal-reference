"""claim-sim: drive the workflows with mock data.

  feed                         deliver change events to Temporal (event bridge stand-in); runs forever
  seed SCENARIO                file a claim from a scenario; prints its claim ID
  change CLAIM_ID WHAT         injury_reported | documents_received | closed_in_claimspro | reopened | reassigned
  tasks [--claim ID]           open adjuster tasks
  decide TASK_ID CHOICE --as ACTOR --roles r1,r2 [--authority CENTS]
  cancel-payment WORKFLOW_ID --as ACTOR
  history CLAIM_ID             business history from the projection table
  run [SCENARIO ...]           run scenarios end to end and check their outcome
  archive [--days N]           run the nightly archive now
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import psycopg

from claims.v1 import payments_pb2, projection_pb2
from claims_runtime import names
from claims_runtime.connection import connect
from claim_sim import bridge, scenarios, world

DSN = os.environ.get("MOCK_DSN", "postgresql://claims:claims@localhost:5432/claims")
SCENARIOS_DIR = Path(os.environ.get("SCENARIOS_DIR", "mock-data/claims"))


async def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(prog="claim-sim", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("feed")
    s = sub.add_parser("seed"); s.add_argument("scenario")
    s = sub.add_parser("change"); s.add_argument("claim_id"); s.add_argument("what", choices=sorted(world.CHANGES))
    s = sub.add_parser("tasks"); s.add_argument("--claim")
    s = sub.add_parser("decide"); s.add_argument("task_id"); s.add_argument("choice")
    s.add_argument("--as", dest="actor", required=True); s.add_argument("--roles", required=True)
    s.add_argument("--authority", type=int, default=0); s.add_argument("--reason", default="")
    s = sub.add_parser("cancel-payment"); s.add_argument("workflow_id"); s.add_argument("--as", dest="actor", required=True)
    s = sub.add_parser("history"); s.add_argument("claim_id")
    s = sub.add_parser("run"); s.add_argument("scenarios", nargs="*")
    s = sub.add_parser("archive"); s.add_argument("--days", type=int, default=0)
    args = p.parse_args(argv)

    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as db:
        if args.cmd == "history":
            return await history(db, args.claim_id)
        if args.cmd == "tasks":
            return await tasks(db, args.claim)
        if args.cmd == "seed":
            files = world.scenario_files(SCENARIOS_DIR)
            print(await world.seed(db, args.scenario, world.load(files[args.scenario])))
            return 0
        if args.cmd == "change":
            await world.change(db, args.claim_id, args.what)
            return 0
        client = await connect()
        if args.cmd == "feed":
            await bridge.run_feed(client, db)
        elif args.cmd == "decide":
            row = await (await db.execute("SELECT workflow_id, decision_id FROM mock.tasks WHERE task_id = %s",
                                          (args.task_id,))).fetchone()
            if not row:
                print(f"No task {args.task_id}")
                return 1
            ok, detail = await world.decide(client, row[0], row[1], args.choice, args.actor, args.roles.split(","),
                                            args.authority, args.reason)
            print(("accepted: " if ok else "rejected: ") + detail)
            return 0 if ok else 1
        elif args.cmd == "cancel-payment":
            await client.get_workflow_handle(args.workflow_id).signal(
                names.SIGNAL_CANCEL_PAYMENT, payments_pb2.CancelPayment(requested_by=args.actor))
        elif args.cmd == "run":
            return await scenarios.run(client, db, SCENARIOS_DIR, args.scenarios)
        elif args.cmd == "archive":
            report = await client.execute_workflow(
                "ArchiveWorkflow", projection_pb2.ArchiveRequest(closed_more_than_days=args.days),
                id=f"claims-archive-manual-{os.getpid()}", task_queue=names.LIFECYCLE_QUEUE,
                result_type=projection_pb2.ArchiveReport)
            print(f"archived {report.rows} rows of {report.claims} claims; "
                  f"histories checked {report.histories_checked}, written {report.histories_written}")
    return 0


async def history(db, claim_id: str) -> int:
    rows = await (await db.execute(
        """SELECT occurred_at, workflow_type, stage_version, seq, step, outcome, actor, reason FROM projection.claim_events
           WHERE claim_id = %s ORDER BY occurred_at, workflow_id, seq""", (claim_id,))).fetchall()
    for at, wtype, v, seq, step, outcome, actor, reason in rows:
        print(f"{at:%H:%M:%S}  v{v:<2} {wtype:<23} {seq:>3}  {step:<60} {outcome:<18} {actor:<9} {reason}")
    return 0


async def tasks(db, claim_id: str | None) -> int:
    rows = await (await db.execute(
        """SELECT task_id, claim_id, kind, required_role, choices, context FROM mock.tasks WHERE status = 'OPEN'
           AND (%s::text IS NULL OR claim_id = %s) ORDER BY created_at""", (claim_id, claim_id))).fetchall()
    for task_id, cid, kind, role, choices, context in rows:
        print(f"{task_id:<10} {cid:<16} {kind:<22} {role:<18} {'|'.join(choices):<40} {context}")
    return 0


def run() -> None:
    sys.exit(asyncio.run(main(sys.argv[1:])))


if __name__ == "__main__":
    run()
