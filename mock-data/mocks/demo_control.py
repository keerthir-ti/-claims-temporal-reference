"""Demo control API (…/demo). NOT one of the mocked systems: it is how the demo, the CLI and the load test
act as "the outside world" (a claim is filed, an adjuster edits it, the customer reopens it…).
Every action changes the fake systems' state and, like the real ClaimsPro, publishes a change event."""
from __future__ import annotations

import json
import re

from mocks import change_feed, world
from mocks.world import HttpError


def handle(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    if path == "/fixtures" and method == "GET":
        return 200, {"fixtures": [{"name": f.stem, "scenario": json.loads(f.read_text())["scenario"]}
                                  for f in sorted(world.FIXTURES_DIR.glob("*.json"))]}
    if path == "/claims" and method == "GET":
        with world.lock:
            return 200, {"claims": [world.public(c) for c in world.all_claims()]}
    if path == "/claims" and method == "POST":
        c = world.create(body)
        if body.get("emitFnol", True):
            change_feed.emit(c, "claim.fnol")      # FNOL filed -> bridge -> Signal-With-Start
        return 201, world.public(c)

    m = re.fullmatch(r"/claims/([^/]+)(/.*)?", path)
    if not m:
        raise HttpError(404, "no route")
    claim_id, sub = m.group(1), m.group(2) or ""
    with world.lock:
        c = world.claim(claim_id)
        if method == "PATCH" and sub == "":
            changed = [k for k, v in body.items()
                       if k in world.CLAIM_FIELDS and k != "claimId" and c.get(k) != v]
            for k in changed:
                c[k] = body[k]
            if changed:
                c["version"] += 1
                # Field NAMES only; values stay in ClaimsPro and are re-read by the workflow.
                change_feed.emit(c, "claim.updated", changedFields=changed)
            return 200, {**world.public(c), "changedFields": changed}
        if method == "POST" and sub == "/reopen":
            c["status"] = "OPEN"
            c["version"] += 1
            change_feed.emit(c, "claim.reopened")
            return 200, world.public(c)
        if method == "POST" and sub == "/cancel-payment-request":
            change_feed.emit(c, "payment.cancel_requested")
            return 202, {"requested": True}
        if method == "POST" and sub == "/stale-write":
            # Replay a write from an older stage, like a late activity retry after re-triage.
            stage = int(body.get("stageVersion", 1))
            try:
                world.guarded_write(c, "claimspro", "/payments", {"X-Stage-Version": str(stage)},
                                    lambda: {"paymentId": "SHOULD-NOT-EXIST"})
                return 200, {"accepted": True, "stageVersion": stage}
            except HttpError as e:
                return 200, {"accepted": False, "status": e.status, "reason": e.message,
                             "currentStageVersion": c["stageVersion"]}
        if method == "GET" and sub == "/ledger":
            return 200, {"claim": world.public(c), "payments": list(c["payments"].values()),
                         "idempotentReplays": sum(1 for a in c["audit"] if a.get("replayed")),
                         "audit": c["audit"][-50:]}
    raise HttpError(404, f"no {method} {sub}")
