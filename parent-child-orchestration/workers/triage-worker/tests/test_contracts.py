"""Reads every golden payload (contracts/golden) through the worker's real data converter, as a Java worker would
send it. The Java tests read the same files."""
from pathlib import Path

from temporalio.api.common.v1 import Payload

from claims import decisions_pb2, events_pb2, payments_pb2, tracks_pb2
from claims_runtime.connection import data_converter

GOLDEN = Path("/app/contracts/golden")


def read(name: str, cls):
    payload = Payload(metadata={"encoding": b"json/protobuf", "messageType": cls.DESCRIPTOR.full_name.encode()},
                      data=(GOLDEN / name).read_bytes())
    return data_converter().payload_converter.from_payloads([payload], [cls])[0]


def test_current_messages_read():
    assert read("ClaimEvent.json", events_pb2.ClaimEvent).type == events_pb2.CLAIM_UPDATED
    assert len(read("TrackRequest.json", tracks_pb2.TrackRequest).buffered_events) == 1
    assert read("TrackResult.json", tracks_pb2.TrackResult).outcome == tracks_pb2.RETRIAGE
    assert read("Decision.json", decisions_pb2.Decision).actor_authority_cents == 5_000_000
    assert read("PaymentRequest.json", payments_pb2.PaymentRequest).amount_cents == 2_500_000


def test_a_newer_writers_message_still_reads():
    r = read("TrackResult.newer-writer.json", tracks_pb2.TrackResult)
    assert r.reason == "written by a newer contract version"
    assert r.outcome == tracks_pb2.TRACK_OUTCOME_UNSPECIFIED  # routed to a person by the parent
