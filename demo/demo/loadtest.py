"""CAT surge load generator.

Files synthetic claims through the mock's demo-control API at a fixed rate. Each one emits an FNOL event that the
bridge turns into Signal-With-Start, so the whole path (bridge -> parent -> TSA -> STP -> grandchild
-> projection) is exercised. Progress is read from Temporal visibility via the ClaimTrack Search
Attribute.

    python -m demo.loadtest --claims 1000 --rate 50
    python -m demo.loadtest --claims 5000 --rate 200 --concurrency 64 --timeout 1800

Existing CAT load scripts can replace the generator: anything that produces FNOL events works.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
import uuid

import httpx

from claims.temporal.client.connection import connect

DEMO_URL = os.environ.get("DEMO_URL", "http://localhost:8090/demo")
TRACKS = ["TSA", "STP", "IS", "MANUAL", "ESCALATION", "SIU", "CLOSED"]


def synthetic_claim(claim_id: str, rng: random.Random) -> dict:
    lob = rng.choices(["AUTO", "HOME"], weights=[7, 3])[0]
    cat = rng.random() < 0.6
    estimate = rng.randint(50_000, 1_400_000)
    return {
        "claim": {
            "claimId": claim_id,
            "lob": lob,
            "state": "TX" if cat else rng.choice(["MA", "RI", "CT", "NH"]),
            "company": "CO-MA",
            "routingChannel": "CAT" if cat else "AGENT",
            "lossType": "HAIL" if cat else "COLLISION",
            "injuryReported": rng.random() < 0.03,
            "estimatedLossCents": estimate,
            "policyActive": True,
            "coverageStatus": "UNCERTAIN" if rng.random() < 0.05 else "CONFIRMED",
        },
        "behaviour": {
            "ai": {"risk": {"fraudScore": round(rng.random() * 0.6, 2), "complexity": round(rng.random() * 0.6, 2)}},
            "eClaims": {"estimate": {"amountCents": int(estimate * 0.8), "delaySeconds": rng.randint(1, 5)}},
            "imageRight": {"documents": {"delaySeconds": rng.randint(2, 6)}},
        },
    }


async def produce(n: int, rate: float, concurrency: int, prefix: str) -> float:
    rng = random.Random(prefix)
    sem = asyncio.Semaphore(concurrency)
    errors = 0
    async with httpx.AsyncClient(base_url=DEMO_URL, timeout=30) as http:

        async def one(i: int):
            nonlocal errors
            async with sem:
                try:
                    r = await http.post("/claims", json=synthetic_claim(f"{prefix}-{i:06d}", rng))
                    r.raise_for_status()
                except Exception:  # noqa: BLE001
                    errors += 1

        start = time.monotonic()
        tasks = []
        for i in range(n):
            tasks.append(asyncio.create_task(one(i)))
            await asyncio.sleep(max(0.0, start + (i + 1) / rate - time.monotonic()))
        await asyncio.gather(*tasks)
        elapsed = time.monotonic() - start
    print(f"submitted {n} FNOLs in {elapsed:.1f}s ({n / elapsed:.1f}/s), errors={errors}")
    return elapsed


async def observe(prefix: str, n: int, timeout: float) -> None:
    client = await connect()
    start = time.monotonic()
    base = f'WorkflowType="ClaimLifecycleWorkflow" AND ClaimId STARTS_WITH "{prefix}-"'
    while True:
        counts = {}
        for track in TRACKS:
            counts[track] = (await client.count_workflows(f'{base} AND ClaimTrack="{track}"')).count
        settled = counts["CLOSED"] + counts["MANUAL"] + counts["ESCALATION"] + counts["SIU"]
        elapsed = time.monotonic() - start
        print(f"[{elapsed:6.0f}s] " + " ".join(f"{k}={v}" for k, v in counts.items()) + f"  settled={settled}/{n}")
        if settled >= n or elapsed > timeout:
            break
        await asyncio.sleep(5)
    print(f"settled {settled}/{n} claims in {time.monotonic() - start:.0f}s "
          f"({settled / max(1.0, time.monotonic() - start):.1f} claims/s end-to-end)")


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--claims", type=int, default=200)
    p.add_argument("--rate", type=float, default=20, help="FNOLs per second")
    p.add_argument("--concurrency", type=int, default=32)
    p.add_argument("--timeout", type=float, default=900)
    p.add_argument("--prefix", default=f"LT{uuid.uuid4().hex[:6].upper()}")
    a = p.parse_args()
    print(f"run prefix {a.prefix}")
    await produce(a.claims, a.rate, a.concurrency, a.prefix)
    await observe(a.prefix, a.claims, a.timeout)


if __name__ == "__main__":
    asyncio.run(main())
