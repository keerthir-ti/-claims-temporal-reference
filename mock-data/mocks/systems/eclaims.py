"""Fake eClaims: vendor dispatch, appraisal, estimates. Base URL = ECLAIMS_URL (…/eclaims)."""
from __future__ import annotations

import re
import uuid

from mocks import change_feed, world
from mocks.world import HttpError

DEFAULT_VENDORS = [{"vendorId": "V-1", "accepts": True}]


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    m = re.fullmatch(r"/claims/([^/]+)(/.*)", path)
    if not m:
        raise HttpError(404, "no route")
    claim_id, sub = m.group(1), m.group(2)
    world.chaos()
    with world.lock:
        c = world.claim(claim_id)
        vendors = world.behaviour(c, "eClaims", "vendors", default=DEFAULT_VENDORS)
        if method == "GET" and sub == "/vendors":
            return 200, {"vendors": [{"vendorId": v["vendorId"]} for v in vendors]}
        if method == "GET" and sub == "/estimate":
            return 200, c["estimate"]
        if method == "POST" and sub == "/dispatch":
            accepts = {v["vendorId"]: v.get("accepts", False) for v in vendors}
            return 200, world.guarded_write(c, "eclaims", sub, headers, lambda: {
                "vendorId": body["vendorId"], "accepted": accepts.get(body["vendorId"], False)})
        if method == "POST" and sub == "/appraisals":
            return 200, world.guarded_write(c, "eclaims", sub, headers, lambda: {
                "appraiserId": f"A-{int(world.stable_fraction(claim_id) * 900) + 100}"})
        if method == "POST" and sub == "/estimates":
            return 200, world.guarded_write(c, "eclaims", sub, headers, lambda: request_estimate(c))
    raise HttpError(404, f"no {method} {sub}")


def request_estimate(c: dict) -> dict:
    cfg = world.behaviour(c, "eClaims", "estimate", default={}) or {}
    amount = cfg.get("amountCents", int(c["estimatedLossCents"] * 0.85))

    def deliver(claim):
        claim["estimate"] = {"ready": True, "amountCents": amount}

    change_feed.emit_later(c["claimId"], cfg.get("delaySeconds", 2), "estimate.received", mutate=deliver,
                           drop=cfg.get("dropEvent", False))
    return {"requestId": f"E-{uuid.uuid4().hex[:8]}"}
