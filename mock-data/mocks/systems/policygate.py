"""Fake Policy Gate: policy / coverage decisions. Base URL = POLICYGATE_URL (…/policygate)."""
from __future__ import annotations

import re

from mocks import world
from mocks.world import HttpError


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    m = re.fullmatch(r"/coverage/([^/]+)", path)
    if method != "GET" or not m:
        raise HttpError(404, "no route")
    world.chaos()
    with world.lock:
        c = world.claim(m.group(1))
        status = c["coverageStatus"] if c["policyActive"] else "DENIED"
        return 200, {"coverageStatus": status, "policyActive": c["policyActive"]}
