"""Temporal client from environment variables; same settings as TemporalConnection.java.

TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_TLS_CERT / TEMPORAL_TLS_KEY (mTLS),
PAYLOAD_KEY / PAYLOAD_KEY_ID (payload encryption, off when unset), METRICS_PORT (0 disables).
"""
from __future__ import annotations

import dataclasses
import os

from temporalio.client import Client, TLSConfig
from temporalio.converter import DataConverter, DefaultFailureConverterWithEncodedAttributes
from temporalio.runtime import PrometheusConfig, Runtime, TelemetryConfig

from claims_runtime.codec import EncryptionCodec


def data_converter() -> DataConverter:
    """Protobuf messages travel as protobuf JSON; with a key set, payloads and failure messages are encrypted."""
    key = os.environ.get("PAYLOAD_KEY")
    if not key:
        return DataConverter.default
    return dataclasses.replace(
        DataConverter.default,
        payload_codec=EncryptionCodec(key, os.environ.get("PAYLOAD_KEY_ID", "key-1")),
        failure_converter_class=DefaultFailureConverterWithEncodedAttributes,
    )


def _runtime(metrics: bool) -> Runtime | None:
    port = os.environ.get("METRICS_PORT", "9464")
    if not metrics or port == "0":
        return None
    return Runtime(telemetry=TelemetryConfig(metrics=PrometheusConfig(bind_address=f"0.0.0.0:{port}")))


async def connect(metrics: bool = False) -> Client:
    tls: TLSConfig | bool = False
    cert = os.environ.get("TEMPORAL_TLS_CERT")
    if cert:
        with open(cert, "rb") as c, open(os.environ["TEMPORAL_TLS_KEY"], "rb") as k:
            tls = TLSConfig(client_cert=c.read(), client_private_key=k.read())
    return await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
        tls=tls,
        data_converter=data_converter(),
        runtime=_runtime(metrics),
    )
