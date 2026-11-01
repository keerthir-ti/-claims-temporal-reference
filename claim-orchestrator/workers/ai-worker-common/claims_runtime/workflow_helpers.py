"""Workflow-side helpers, same behaviour as the Java ones in workers/worker-common:

HistoryRecorder      business-history rows 1..n per run, ending with a closing row; each write awaited
DecisionDesk  human decisions: adjuster task in ClaimsPro, validated Update, deadline and escalation
run_step      retry window, then an operations task (RETRY or STOP)

Import inside ``workflow.unsafe.imports_passed_through()``.
"""
from __future__ import annotations

from datetime import timedelta
from typing import Awaitable, Callable, TypeVar

from temporalio import workflow
from temporalio.common import RetryPolicy, SearchAttributeKey
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError

from claims import common_pb2, decisions_pb2, projection_pb2, systems_pb2
from claims_runtime import names

T = TypeVar("T")
BUSINESS_ERRORS = ["NotFound", "Refused", "StaleStage"]
DEADLINE_PASSED = "DEADLINE_PASSED"
OPEN_DECISION = SearchAttributeKey.for_keyword("OpenDecision")


def retry_policy() -> RetryPolicy:
    return RetryPolicy(initial_interval=timedelta(seconds=1), backoff_coefficient=2.0,
                       maximum_interval=timedelta(seconds=60), non_retryable_error_types=BUSINESS_ERRORS)


def retry_window(policy: decisions_pb2.DecisionPolicy) -> timedelta:
    return timedelta(seconds=policy.retry_window_seconds or 3600)


async def load_policy(claim_id: str) -> decisions_pb2.DecisionPolicy:
    return await workflow.execute_activity(
        "Policy.load", decisions_pb2.PolicyRequest(claim_id=claim_id), task_queue=names.LIFECYCLE_QUEUE,
        start_to_close_timeout=timedelta(seconds=10), retry_policy=retry_policy(),
        result_type=decisions_pb2.DecisionPolicy)


async def claimspro(name: str, arg, result_type: type[T], window: timedelta | None) -> T:
    """Calls a ClaimsPro activity on the rate-limited claimspro-activities queue (Java worker)."""
    return await workflow.execute_activity(
        f"ClaimsPro.{name}", arg, task_queue=names.CLAIMSPRO_QUEUE, start_to_close_timeout=timedelta(seconds=30),
        schedule_to_close_timeout=window, retry_policy=retry_policy(), result_type=result_type)


def is_business(e: ActivityError) -> bool:
    cause = e.cause
    return isinstance(cause, ApplicationError) and (cause.non_retryable or cause.type in BUSINESS_ERRORS)


class HistoryRecorder:
    def __init__(self, claim_id: str, workflow_type: str) -> None:
        self._claim_id = claim_id
        self._type = workflow_type
        self.seq = 0

    async def row(self, stage_version: int, step: str, *, outcome: str = "", trigger: str = "", actor: str = "",
                  reason: str = "", refs: dict[str, str] | None = None, closing: bool = False) -> None:
        self.seq += 1
        info = workflow.info()
        await workflow.execute_activity(
            "Projection.record",
            projection_pb2.ProjectionEvent(
                claim_id=self._claim_id, workflow_id=info.workflow_id, run_id=info.run_id, workflow_type=self._type,
                seq=self.seq, stage_version=stage_version, step=step, outcome=outcome, trigger_id=trigger,
                actor=actor, reason=reason, refs=refs or {}, closing=closing,
                occurred_at_ms=int(workflow.time() * 1000)),
            task_queue=names.LIFECYCLE_QUEUE, start_to_close_timeout=timedelta(seconds=10),
            retry_policy=retry_policy(), result_type=systems_pb2.Done)

    async def closing(self, stage_version: int, outcome: str) -> None:
        await self.row(stage_version, "run.closed", outcome=outcome, closing=True)


class DecisionDesk:
    """See DecisionDesk.java. ``validate`` and ``handle`` back the workflow's ``decide`` Update."""

    def __init__(self, claim_id: str, stage_version: int, recorder: HistoryRecorder) -> None:
        self._claim_id = claim_id
        self._stage = stage_version
        self._recorder = recorder
        self._open: dict[str, decisions_pb2.OpenDecision] = {}
        self._decided: dict[str, decisions_pb2.DecisionResult] = {}
        self._counter = 0
        self.policy = decisions_pb2.DecisionPolicy()

    async def decide(self, kind: int, choices: list[str], context: str = "", required_authority_cents: int = 0,
                     requested_by: str = "", interrupt: Callable[[], bool] = lambda: False
                     ) -> decisions_pb2.DecisionResult | None:
        kind_name = decisions_pb2.DecisionKind.Name(kind)
        rule = self.policy.rules.get(kind_name, decisions_pb2.DecisionRule())
        roles = [rule.role, *rule.escalation_roles]
        workflow_id = workflow.info().workflow_id
        for level, role in enumerate(roles):
            self._counter += 1
            decision_id = f"{workflow_id}:v{self._stage}:{kind_name.lower()}:{self._counter}"
            deadline = rule.deadline_seconds
            od = decisions_pb2.OpenDecision(
                decision_id=decision_id, kind=kind, claim_id=self._claim_id, workflow_id=workflow_id,
                choices=choices, required_role=role, required_authority_cents=required_authority_cents,
                requested_by=requested_by, level=level, context=context,
                due_at_ms=int(workflow.time() * 1000) + deadline * 1000 if deadline else 0)
            # Open before the task exists: an adjuster may answer as soon as the task appears.
            self._open[decision_id] = od
            task = await claimspro("createTask", systems_pb2.TaskRequest(
                step=self._step(f"task:{decision_id}"), decision=od), systems_pb2.TaskRef, timedelta(hours=1))
            od.task_id = task.task_id
            workflow.upsert_search_attributes([OPEN_DECISION.value_set(kind_name)])
            await self._recorder.row(self._stage, f"decision.open:{decision_id}", outcome=role,
                                     reason=context, refs={"task_id": task.task_id})
            try:
                await workflow.wait_condition(lambda: decision_id in self._decided or interrupt(),
                                              timeout=timedelta(seconds=deadline) if deadline else None)
            except TimeoutError:
                pass
            self._open.pop(decision_id, None)
            if not self._open:
                workflow.upsert_search_attributes([OPEN_DECISION.value_unset()])
            result = self._decided.get(decision_id)
            if result is not None:
                await self._close_task(task.task_id, result.choice)
                await self._recorder.row(self._stage, f"decision:{decision_id}", outcome=result.choice,
                                         actor=result.decided_by, trigger=decision_id, refs={"task_id": task.task_id})
                return result
            if interrupt():
                await self._close_task(task.task_id, "WITHDRAWN")
                await self._recorder.row(self._stage, f"decision.withdrawn:{decision_id}", outcome="WITHDRAWN",
                                         refs={"task_id": task.task_id})
                return None
            await self._close_task(task.task_id, "ESCALATED")
            await self._recorder.row(self._stage, f"decision.deadline:{decision_id}", outcome=DEADLINE_PASSED,
                                     refs={"task_id": task.task_id})
        return decisions_pb2.DecisionResult(choice=DEADLINE_PASSED)

    def validate(self, d: decisions_pb2.Decision) -> None:
        if d.decision_id in self._decided:
            return  # a repeated decision ID returns the first result
        od = self._open.get(d.decision_id)
        if od is None:
            raise ValueError(f"No open decision {d.decision_id}")
        if d.choice not in od.choices:
            raise ValueError(f"Choice {d.choice} is not one of {list(od.choices)}")
        if od.required_role and od.required_role not in d.actor_roles:
            raise ValueError(f"Role {od.required_role} required")
        if od.required_authority_cents and d.actor_authority_cents < od.required_authority_cents:
            raise ValueError(f"Authority of {od.required_authority_cents} cents required")
        if od.requested_by and od.requested_by == d.actor_id:
            raise ValueError("The requester cannot approve their own request")

    def handle(self, d: decisions_pb2.Decision) -> decisions_pb2.DecisionResult:
        if d.decision_id not in self._decided:
            self._decided[d.decision_id] = decisions_pb2.DecisionResult(
                decision_id=d.decision_id, choice=d.choice, decided_by=d.actor_id)
        return self._decided[d.decision_id]

    async def withdraw_all(self) -> None:
        for od in list(self._open.values()):
            await self._close_task(od.task_id, "WITHDRAWN")
        self._open.clear()

    async def _close_task(self, task_id: str, outcome: str) -> None:
        await claimspro("closeTask", systems_pb2.CloseTaskRequest(
            step=self._step(f"task.close:{task_id}"), task_id=task_id, outcome=outcome), systems_pb2.Done, None)

    def _step(self, name: str) -> common_pb2.StepRef:
        return common_pb2.StepRef(claim_id=self._claim_id, stage_version=self._stage, step=name)


class Stopped(Exception):
    """A person chose to stop retrying a step."""


async def run_step(desk: DecisionDesk, step: str, call: Callable[[], Awaitable[T]]) -> T:
    """Business failures go back to the caller; after the retry window an operations task decides."""
    while True:
        try:
            return await call()
        except ActivityError as e:
            if is_business(e) or isinstance(e.cause, CancelledError):
                raise
            result = await desk.decide(decisions_pb2.OPERATIONS_EXCEPTION, ["RETRY", "STOP"],
                                       f"{step} failed after its retry window: {e.cause}")
            if result is None or result.choice not in ("RETRY", DEADLINE_PASSED):
                raise Stopped(step) from e
