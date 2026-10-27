"""JSON logs with the same field names the Java SDK puts in the MDC (WorkflowId, RunId,
WorkflowType, TaskQueue, ActivityType, Attempt), so one Kibana query covers both languages."""
from __future__ import annotations

import logging
import os
import sys

from pythonjsonlogger.json import JsonFormatter
from temporalio import activity, workflow

_WORKFLOW_KEYS = {
    "workflow_id": "WorkflowId", "run_id": "RunId", "workflow_type": "WorkflowType",
    "task_queue": "TaskQueue", "namespace": "Namespace", "attempt": "Attempt",
}
_ACTIVITY_KEYS = {
    "workflow_id": "WorkflowId", "workflow_run_id": "RunId", "workflow_type": "WorkflowType",
    "task_queue": "TaskQueue", "activity_id": "ActivityId", "activity_type": "ActivityType",
    "attempt": "Attempt", "namespace": "Namespace",
}


class TemporalContextFilter(logging.Filter):
    """Flattens temporalio's `temporal_workflow` / `temporal_activity` extras into top-level fields."""

    def filter(self, record: logging.LogRecord) -> bool:
        for attr, mapping in (("temporal_workflow", _WORKFLOW_KEYS), ("temporal_activity", _ACTIVITY_KEYS)):
            ctx = getattr(record, attr, None)
            if isinstance(ctx, dict):
                for src, dst in mapping.items():
                    if src in ctx:
                        setattr(record, dst, ctx[src])
                delattr(record, attr)
        return True


def configure(service: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(TemporalContextFilter())
    if os.environ.get("LOG_FORMAT", "json") == "json":
        handler.setFormatter(JsonFormatter(
            "%(levelname)s %(name)s %(message)s",
            timestamp="@timestamp",
            rename_fields={"levelname": "level", "name": "logger_name"},
            static_fields={"service": service, "language": "python"},
        ))
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(WorkflowId)s] %(message)s", defaults={"WorkflowId": "-"}))
    # Context goes into structured fields (above), not appended to the message text.
    workflow.logger.workflow_info_on_message = False
    activity.logger.activity_info_on_message = False
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    logging.getLogger("temporalio").setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
