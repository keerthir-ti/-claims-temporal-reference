"""AES-256-GCM payload encryption, byte-compatible with PayloadEncryptionCodec.java.

Data = 12-byte nonce + ciphertext + 16-byte tag; the plaintext is the serialized original Payload.
"""
from __future__ import annotations

import base64
import os
from typing import Sequence

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from temporalio.api.common.v1 import Payload
from temporalio.converter import PayloadCodec

ENCODING = b"binary/encrypted"
KEY_ID = "encryption-key-id"


class EncryptionCodec(PayloadCodec):
    def __init__(self, base64_key: str, key_id: str) -> None:
        key = base64.b64decode(base64_key)
        if len(key) != 32:
            raise ValueError("PAYLOAD_KEY must be 32 bytes, base64 encoded")
        self._aes = AESGCM(key)
        self._key_id = key_id

    async def encode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            nonce = os.urandom(12)
            out.append(Payload(
                metadata={"encoding": ENCODING, KEY_ID: self._key_id.encode()},
                data=nonce + self._aes.encrypt(nonce, p.SerializeToString(), None),
            ))
        return out

    async def decode(self, payloads: Sequence[Payload]) -> list[Payload]:
        out = []
        for p in payloads:
            if p.metadata.get("encoding") != ENCODING:
                out.append(p)
                continue
            key_id = p.metadata.get(KEY_ID, b"").decode()
            if key_id != self._key_id:
                raise ValueError(f"Unknown payload key id {key_id}")
            decoded = Payload()
            decoded.ParseFromString(self._aes.decrypt(p.data[:12], p.data[12:], None))
            out.append(decoded)
        return out
