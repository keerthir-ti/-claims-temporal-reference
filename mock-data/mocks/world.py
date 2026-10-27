"""Shared state behind every fake system: one dict per claim, plus the two guarantees the real
systems must give the workflows (and that the demo exercises):

  * stage fencing   - a write whose X-Stage-Version is older than the claim's stage gets 409
  * idempotency     - a repeated Idempotency-Key returns the original result ("replayed": true)

Per-claim behaviour (AI scores, vendor answers, delays, failures) comes from the fixture's
"behaviour" block, grouped by the system that produces it.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import threading
from datetime import datetime, timezone
from pathlib import Path

FIXTURES_DIR = Path(os.environ.get("FIXTURES_DIR", Path(__file__).resolve().parents[1] / "fixtures" / "claims"))
FAILURE_RATE = float(os.environ.get("MOCK_FAILURE_RATE", "0"))
LATENCY_MS = int(os.environ.get("MOCK_LATENCY_MS", "0"))

CLAIM_FIELDS = (
    "claimId", "lob", "state", "company", "routingChannel", "lossType", "injuryReported",
    "estimatedLossCents", "policyActive", "coverageStatus",
)

lock = threading.RLock()
_claims: dict[str, dict] = {}


class HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


def now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_fraction(text: str) -> float:
    return int(hashlib.sha256(text.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF


def claim(claim_id: str) -> dict:
    c = _claims.get(claim_id)
    if c is None:
        raise HttpError(404, f"claim {claim_id} not found")
    return c


def all_claims() -> list[dict]:
    return list(_claims.values())


def public(c: dict) -> dict:
    out = {k: c.get(k) for k in CLAIM_FIELDS}
    out.update(version=c["version"], status=c["status"],
               currentStageVersion=c["stageVersion"], currentTrack=c["track"])
    return out


def behaviour(c: dict, *path, default=None):
    node = c.get("behaviour", {})
    for key in path:
        if not isinstance(node, dict) or key not in node:
            return default
        node = node[key]
    return node


def create(body: dict) -> dict:
    """body: {"fixture": "CLM-1001", "claimId": "..."} or {"claim": {...}, "behaviour": {...}}."""
    if "fixture" in body:
        fixture = json.loads((FIXTURES_DIR / f"{body['fixture']}.json").read_text())
        data = dict(fixture["claim"])
        beh = fixture.get("behaviour", {})
    else:
        data = dict(body["claim"])
        beh = body.get("behaviour", {})
    if body.get("claimId"):
        data["claimId"] = body["claimId"]
    c = {k: data.get(k) for k in CLAIM_FIELDS}
    c.update(version=1, status="OPEN", stageVersion=0, track=None, pii=data.get("pii", {}), behaviour=beh,
             idempotency={}, payments={}, estimate={"ready": False}, documents={"received": 0},
             chaosUsed=set(), audit=[])
    with lock:
        _claims[c["claimId"]] = c
    return c


def chaos() -> None:
    """Global failure injection for the system APIs (never for the demo-control API)."""
    if LATENCY_MS:
        import time
        time.sleep(random.uniform(0, LATENCY_MS) / 1000)
    if FAILURE_RATE and random.random() < FAILURE_RATE:
        raise HttpError(503, "simulated transient failure")


def guarded_write(c: dict, system: str, op: str, headers, apply):
    """Stage fence + idempotency around one write. `apply()` performs it and returns the result."""
    stage = headers.get("X-Stage-Version")
    if stage is not None and int(stage) < c["stageVersion"]:
        raise HttpError(409, f"stale stage {stage}; claim is at {c['stageVersion']}")
    key = headers.get("Idempotency-Key")
    store_key = f"{system}:{key}" if key else None
    if store_key and store_key in c["idempotency"]:
        c["audit"].append({"at": now(), "system": system, "op": op, "key": key, "replayed": True})
        return {**c["idempotency"][store_key], "replayed": True}
    result = apply()
    if store_key:
        c["idempotency"][store_key] = result
    c["audit"].append({"at": now(), "system": system, "op": op, "key": key, "replayed": False})
    return {**result, "replayed": False}
