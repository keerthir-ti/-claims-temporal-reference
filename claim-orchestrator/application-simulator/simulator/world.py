"""Mock claims systems: claims from scenario files, changes made "in ClaimsPro / ImageRight", adjuster
decisions sent "through Kong"."""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import yaml
from temporalio.client import Client, WorkflowUpdateFailedError

from claims import decisions_pb2
from claims_runtime import names



def scenario_files(scenarios_dir: Path) -> dict[str, Path]:
    return {p.stem: p for p in sorted(scenarios_dir.glob("*.yaml"))}


def load(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


async def seed(db, scenario: str, doc: dict, claim_id: str | None = None) -> str:
    claim_id = claim_id or f"CLM-{scenario[:2]}-{uuid.uuid4().hex[:6].upper()}"
    c = doc["claim"]
    await db.execute(
        """INSERT INTO mock.claims (claim_id, scenario, line_of_business, company, channel, state,
             claimed_amount_cents, prior_claims, adjuster_id, behaviour)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (claim_id, scenario, c["line_of_business"], c["company"], c["channel"], c["state"],
         c.get("claimed_amount_cents", 0), c.get("prior_claims", 0), c.get("adjuster_id", ""),
         json.dumps(doc.get("behaviour", {}))))
    await event(db, claim_id, "CLAIM_CREATED", "ClaimsPro")
    return claim_id


async def event(db, claim_id: str, type_: str, source: str) -> None:
    await db.execute("INSERT INTO mock.change_events (event_id, claim_id, type, source_system) VALUES (%s, %s, %s, %s)",
                     (f"{source.lower()}-{type_.lower()}-{uuid.uuid4().hex[:10]}", claim_id, type_, source))


CHANGES = {
    "injury_reported": ("UPDATE mock.claims SET injury_reported = true, change_version = change_version + 1 "
                        "WHERE claim_id = %s", "CLAIM_UPDATED", "ClaimsPro"),
    "documents_received": ("UPDATE mock.claims SET documents_received = true WHERE claim_id = %s",
                           "DOCUMENTS_RECEIVED", "ImageRight"),
    "closed_in_claimspro": ("UPDATE mock.claims SET status = 'CLOSED', closed_reason = 'closed in ClaimsPro' "
                            "WHERE claim_id = %s", "CLAIM_CLOSED", "ClaimsPro"),
    "reopened": ("UPDATE mock.claims SET status = 'OPEN', change_version = change_version + 1 WHERE claim_id = %s",
                 "CLAIM_REOPENED", "ClaimsPro"),
    "payment_cancelled": ("UPDATE mock.claims SET updated_at = now() WHERE claim_id = %s",
                          "PAYMENT_CANCEL_REQUESTED", "ClaimsPro"),
    "reassigned": ("UPDATE mock.claims SET adjuster_id = 'adj-reassigned' WHERE claim_id = %s",
                   "CLAIM_REASSIGNED", "ClaimsPro"),
}


async def change(db, claim_id: str, what: str) -> None:
    sql, type_, source = CHANGES[what]
    await db.execute(sql, (claim_id,))
    await event(db, claim_id, type_, source)


async def open_task(db, claim_id: str, kind: str):
    return await (await db.execute(
        """SELECT task_id, decision_id, workflow_id, choices, required_role FROM mock.tasks
           WHERE claim_id = %s AND kind = %s AND status = 'OPEN' ORDER BY created_at DESC LIMIT 1""",
        (claim_id, kind))).fetchone()


async def decide(client: Client, workflow_id: str, decision_id: str, choice: str, actor: str, roles: list[str],
                 authority_cents: int = 0, reason: str = "") -> tuple[bool, str]:
    """Sends the decide Update (decision ID = Update ID). Returns (accepted, detail)."""
    decision = decisions_pb2.Decision(decision_id=decision_id, choice=choice, actor_id=actor, actor_roles=roles,
                                      actor_authority_cents=authority_cents, reason=reason)
    try:
        result = await client.get_workflow_handle(workflow_id).execute_update(
            names.UPDATE_DECIDE, decision, id=decision_id, result_type=decisions_pb2.DecisionResult)
        return True, f"{result.choice} by {result.decided_by}"
    except WorkflowUpdateFailedError as e:
        return False, str(e.cause.message if e.cause else e)
