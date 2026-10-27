"""Temporal client bootstrap shared by workers, CLI and load test.

Same env vars as the Java side (TemporalSupport.java), so one .env drives both languages:
TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_API_KEY or TEMPORAL_TLS_CERT/TEMPORAL_TLS_KEY,
METRICS_PORT.
"""
from __future__ import annotations

import os

from temporalio.client import Client, TLSConfig
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig


def _runtime(metrics: bool) -> Runtime | None:
    if not metrics:
        return None
    port = os.environ.get("METRICS_PORT", "9464")
    return Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=f"0.0.0.0:{port}")))


async def connect(metrics: bool = False) -> Client:
    tls: TLSConfig | bool = False
    api_key = os.environ.get("TEMPORAL_API_KEY") or None
    cert = os.environ.get("TEMPORAL_TLS_CERT")
    if api_key:
        tls = True
    elif cert:
        with open(cert, "rb") as c, open(os.environ["TEMPORAL_TLS_KEY"], "rb") as k:
            tls = TLSConfig(client_cert=c.read(), client_private_key=k.read())
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        api_key=api_key,
        tls=tls,
        # Contract models are pydantic (generated from contracts/schema); JSON on the wire is
        # identical to what Java's Jackson converter writes/reads.
        data_converter=pydantic_data_converter,
        runtime=_runtime(metrics),
    )
