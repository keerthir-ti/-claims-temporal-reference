"""Fake AI services (triage risk model, coverage-analysis model). Base URL = AI_URL (…/ai)."""
from __future__ import annotations

from mocks import world
from mocks.world import HttpError


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    if method != "POST":
        raise HttpError(404, "no route")
    world.chaos()
    with world.lock:
        c = world.claim(body["claimId"])
        if path == "/risk/score":
            scores = world.behaviour(c, "ai", "risk") or {
                "fraudScore": round(world.stable_fraction(c["claimId"] + "f") * 0.5, 2),
                "complexity": round(world.stable_fraction(c["claimId"] + "c") * 0.6, 2),
            }
            return 200, {**scores, "model": "mock-risk-v1"}
        if path == "/coverage/analyze":
            return 200, {"needsDocuments": c["coverageStatus"] == "UNCERTAIN",
                         "likelyCovered": world.behaviour(c, "ai", "coverage", "likelyCovered", default=True),
                         "model": "mock-coverage-llm-v1"}
    raise HttpError(404, "no route")
