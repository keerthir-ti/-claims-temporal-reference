"""MOCK ONLY: HTTP client for the mock-data service (scripted claims standing in for ClaimsPro,
ImageRight and the AI models). Same rules as MockSystemsClient.java, and they must survive in the
real activities: writes carry X-Stage-Version + Idempotency-Key; 404/409/422 are non-retryable,
everything else is retried by Temporal."""
from __future__ import annotations

import os

import httpx
from temporalio.exceptions import ApplicationError

from claims.temporal.activities.errors import BUSINESS_RULE, NOT_FOUND, STALE_STAGE
from claims.temporal.activities.types import StepRef

_NON_RETRYABLE = {404: NOT_FOUND, 409: STALE_STAGE, 422: BUSINESS_RULE}


class MockSystemsClient:
    def __init__(self, base_url: str | None = None):
        self._http = httpx.AsyncClient(
            base_url=(base_url or os.environ.get("MOCK_SYSTEMS_URL", "http://localhost:8090")).rstrip("/"),
            timeout=10.0)

    async def get(self, path: str) -> dict:
        return self._check(await self._http.get(path))

    async def post(self, path: str, body: dict | None = None, ref: StepRef | None = None) -> dict:
        headers = {}
        if ref is not None:
            headers = {"X-Stage-Version": str(ref.stageVersion), "Idempotency-Key": ref.key}
        return self._check(await self._http.post(path, json=body or {}, headers=headers))

    @staticmethod
    def _check(resp: httpx.Response) -> dict:
        if resp.status_code in _NON_RETRYABLE:
            raise ApplicationError(resp.text, type=_NON_RETRYABLE[resp.status_code], non_retryable=True)
        if resp.status_code >= 300:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
        return resp.json()
