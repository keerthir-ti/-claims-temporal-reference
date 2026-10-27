"""Fake ClaimsPro: the claims system of record. Base URL = CLAIMSPRO_URL (…/claimspro)."""
from __future__ import annotations

import logging
import re
import uuid

from mocks import change_feed, world
from mocks.world import HttpError

log = logging.getLogger("mock.claimspro")


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    m = re.fullmatch(r"/claims/([^/]+)(/.*)?", path)
    if not m:
        raise HttpError(404, "no route")
    claim_id, sub = m.group(1), m.group(2) or ""
    world.chaos()
    with world.lock:
        c = world.claim(claim_id)
        if method == "GET" and sub == "":
            return 200, world.public(c)
        if method == "POST" and sub == "/stage":
            return 200, begin_stage(c, body)
        if method == "POST":
            return write(c, sub, body, headers)
    raise HttpError(404, f"no {method} {sub}")


def begin_stage(c: dict, body: dict) -> dict:
    stage = int(body["stageVersion"])
    if stage < c["stageVersion"]:
        raise HttpError(409, f"stage {stage} older than current {c['stageVersion']}")
    c["stageVersion"], c["track"] = stage, body.get("track")
    c["audit"].append({"at": world.now(), "system": "claimspro", "op": "stage", "stageVersion": stage, "track": c["track"]})
    return world.public(c)


def write(c: dict, sub: str, body: dict, headers) -> tuple[int, dict]:
    if sub == "/reserves":
        result = world.guarded_write(c, "claimspro", sub, headers, lambda: {
            "reserveId": f"R-{uuid.uuid4().hex[:8]}", "amountCents": c["estimatedLossCents"]})
    elif sub == "/payments":
        def pay():
            p = {"paymentId": f"P-{uuid.uuid4().hex[:8]}", "amountCents": body["amountCents"], "status": "ISSUED"}
            c["payments"][p["paymentId"]] = p
            return dict(p)
        result = world.guarded_write(c, "claimspro", sub, headers, pay)
        if world.behaviour(c, "claimsPro", "failPaymentAfterCommitOnce") and "payAfterCommit" not in c["chaosUsed"]:
            c["chaosUsed"].add("payAfterCommit")
            log.warning("CHAOS: payment %s committed, now failing the response", result["paymentId"])
            raise HttpError(503, "connection reset after commit (simulated)")
    elif m := re.fullmatch(r"/payments/([^/]+)/(finalize|cancel)", sub):
        def settle():
            p = c["payments"].get(m.group(1))
            if p is None:
                raise HttpError(422, "unknown payment")
            p["status"] = "CLEARED" if m.group(2) == "finalize" else "CANCELLED"
            return dict(p)
        result = world.guarded_write(c, "claimspro", sub, headers, settle)
    elif sub == "/close":
        def close():
            c["status"] = "CLOSED"
            c["version"] += 1
            return {"status": "CLOSED"}
        result = world.guarded_write(c, "claimspro", sub, headers, close)
    elif sub == "/coverage":
        def coverage():
            c["coverageStatus"] = body["coverageStatus"]
            c["version"] += 1
            change_feed.emit(c, "claim.updated", changedFields=["coverageStatus"])
            return {"coverageStatus": c["coverageStatus"]}
        result = world.guarded_write(c, "claimspro", sub, headers, coverage)
    else:
        raise HttpError(404, f"no POST {sub}")
    return 200, result
