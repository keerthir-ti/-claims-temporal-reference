"""Fake Comm Hub: customer communications. Base URL = COMMHUB_URL (…/commhub)."""
from __future__ import annotations

import uuid

from mocks import world
from mocks.world import HttpError


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    if method != "POST" or path != "/contacts":
        raise HttpError(404, "no route")
    world.chaos()
    with world.lock:
        c = world.claim(body["claimId"])
        return 200, world.guarded_write(c, "commhub", "/contacts", headers, lambda: {
            "contactId": f"M-{uuid.uuid4().hex[:8]}",
            "reachable": world.behaviour(c, "commHub", "reachable", default=True)})
