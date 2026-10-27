"""Fake ImageRight: document management. Base URL = IMAGERIGHT_URL (…/imageright)."""
from __future__ import annotations

import re

from mocks import change_feed, world
from mocks.world import HttpError


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    m = re.fullmatch(r"/claims/([^/]+)(/.*)", path)
    if not m:
        raise HttpError(404, "no route")
    claim_id, sub = m.group(1), m.group(2)
    world.chaos()
    with world.lock:
        c = world.claim(claim_id)
        if method == "GET" and sub == "/documents":
            return 200, c["documents"]
        if method == "POST" and sub == "/document-requests":
            return 200, world.guarded_write(c, "imageright", sub, headers, lambda: request_documents(c))
    raise HttpError(404, f"no {method} {sub}")


def request_documents(c: dict) -> dict:
    cfg = world.behaviour(c, "imageRight", "documents", default={}) or {}

    def arrive(claim):
        claim["documents"] = {"received": claim["documents"]["received"] + 1}

    if not cfg.get("never"):
        change_feed.emit_later(c["claimId"], cfg.get("delaySeconds", 5), "document.received", mutate=arrive,
                               drop=cfg.get("dropEvent", False), documentType="POLICE_REPORT")
    return {"requested": True}
