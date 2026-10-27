"""Golden payload tests. The Java side runs the same files (ContractGoldenTest.java), so a payload
that decodes here and there is byte-for-byte the cross-language contract."""
import json
from pathlib import Path

import pytest
from temporalio.contrib.pydantic import pydantic_data_converter

from claims.contracts import models

GOLDEN = Path(__file__).resolve().parents[2] / "contracts" / "golden"
converter = pydantic_data_converter.payload_converter


def _drop_nulls(value):
    # Python writes explicit nulls, Java omits them (NON_NULL). Both mean "absent".
    if isinstance(value, dict):
        return {k: _drop_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [_drop_nulls(v) for v in value]
    return value


@pytest.mark.parametrize("path", sorted(GOLDEN.glob("*.json")), ids=lambda p: p.stem)
def test_golden_round_trip(path: Path):
    model_type = getattr(models, path.stem)
    original = json.loads(path.read_text())
    decoded = converter.from_payloads(converter.to_payloads([original]), [model_type])[0]
    assert isinstance(decoded, model_type)
    encoded = json.loads(converter.to_payloads([decoded])[0].data)
    assert _drop_nulls(encoded) == original


def test_forward_compatible_read():
    raw = json.loads((GOLDEN / "forward-compat" / "TrackResult.json").read_text())
    decoded = converter.from_payloads(converter.to_payloads([raw]), [models.TrackResult])[0]
    assert decoded.outcome == models.TrackOutcome.UNKNOWN  # unknown enum -> UNKNOWN, no exception
    assert decoded.track == models.Track.STP
    assert not hasattr(decoded, "riskBand")  # unknown field ignored
