"""Simulates ClaimsPro's change-data-capture feed: raw ClaimsPro-shaped events (including PII on
purpose) posted to the Claims Event Bridge, which must allowlist them and strip the PII."""
from __future__ import annotations

import json
import logging
import os
import threading
import urllib.error
import urllib.request
import uuid

from mocks import world

log = logging.getLogger("mock.change_feed")
BRIDGE_URL = os.environ.get("BRIDGE_URL", "http://localhost:8085")


def emit(c: dict, event_type: str, **extra) -> None:
    raw = {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "claimId": c["claimId"],
        "version": c["version"],
        "occurredAt": world.now(),
        "policyHolderName": c.get("pii", {}).get("policyHolderName"),
        **extra,
    }

    def send():
        req = urllib.request.Request(f"{BRIDGE_URL}/events", data=json.dumps(raw).encode(),
                                     headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                log.info("event %s %s -> bridge %s", event_type, c["claimId"], resp.status)
        except urllib.error.HTTPError as e:
            log.info("event %s %s -> bridge %s", event_type, c["claimId"], e.code)
        except Exception as e:  # noqa: BLE001
            log.warning("event %s %s not delivered: %s", event_type, c["claimId"], e)

    threading.Thread(target=send, daemon=True).start()


def emit_later(claim_id: str, delay_s: float, event_type: str, mutate=None, drop: bool = False, **extra) -> None:
    """Something happens in the outside world after a delay (estimate ready, documents arrive).
    `drop=True` changes the data but loses the event: only a backstop poll can notice."""
    def fire():
        with world.lock:
            c = world.claim(claim_id)
            if mutate:
                mutate(c)
            c["version"] += 1
        if drop:
            log.warning("DROPPED %s for %s (backstop poll must recover it)", event_type, claim_id)
        else:
            emit(c, event_type, **extra)

    threading.Timer(delay_s, fire).start()
