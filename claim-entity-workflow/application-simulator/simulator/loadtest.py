"""Catastrophe surge: files N claims of one scenario at a fixed rate and measures how long each takes to close.

Every claim must close with a complete business record; failures and timeouts are reported.
"""
from __future__ import annotations

import asyncio
import statistics
import time
from pathlib import Path

from temporalio.client import Client

from simulator import scenarios, world


async def run(client: Client, db, scenarios_dir: Path, scenario: str, claims: int, rate: float) -> int:
    doc = world.load(world.scenario_files(scenarios_dir)[scenario])
    started = time.monotonic()
    filed: dict[str, float] = {}
    for i in range(claims):
        claim_id = await world.seed(db, scenario, doc)
        filed[claim_id] = time.monotonic()
        await asyncio.sleep(max(0.0, (i + 1) / rate - (time.monotonic() - started)))
    print(f"  filed {claims} claims in {time.monotonic() - started:.0f} s ({rate:g} per second)")

    durations: list[float] = []
    open_claims = set(filed)
    deadline = time.monotonic() + max(300, claims * 2)
    while open_claims and time.monotonic() < deadline:
        rows = await (await db.execute(
            "SELECT claim_id FROM projection.claim_stage WHERE stage = 'STAGE_CLOSED' AND claim_id = ANY(%s)",
            (list(open_claims),))).fetchall()
        for (claim_id,) in rows:
            if await scenarios.completed(client, claim_id):
                durations.append(time.monotonic() - filed[claim_id])
                open_claims.discard(claim_id)
        await asyncio.sleep(1)
    incomplete = [c for c in filed if c not in open_claims and await scenarios.record_problems(db, c)]
    total = time.monotonic() - started
    durations.sort()
    p95 = durations[int(len(durations) * 0.95) - 1] if durations else 0
    print(f"  closed {len(durations)}/{claims} in {total:.0f} s; claim duration median "
          f"{statistics.median(durations) if durations else 0:.1f} s, p95 {p95:.1f} s")
    print(f"  not closed: {len(open_claims)}; incomplete records: {len(incomplete)}")
    payments = await (await db.execute(
        "SELECT count(*), count(DISTINCT claim_id) FROM mock.payments WHERE claim_id = ANY(%s)", (list(filed),))).fetchone()
    print(f"  payments: {payments[0]} for {payments[1]} claims (one per claim expected)")
    return 0 if not open_claims and not incomplete and payments[0] == payments[1] == claims else 1
