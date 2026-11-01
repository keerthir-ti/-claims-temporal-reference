"""Worker Versioning: track workflows are short-lived, so they are Pinned to the build they started on."""
from __future__ import annotations

import os

from temporalio.common import VersioningBehavior, WorkerDeploymentVersion
from temporalio.worker import WorkerDeploymentConfig


def pinned(deployment: str) -> WorkerDeploymentConfig | None:
    if os.environ.get("WORKER_VERSIONING", "true").lower() != "true":
        return None
    return WorkerDeploymentConfig(
        version=WorkerDeploymentVersion(deployment_name=deployment, build_id=os.environ.get("BUILD_ID", "dev")),
        use_worker_versioning=True,
        default_versioning_behavior=VersioningBehavior.PINNED,
    )
