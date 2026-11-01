"""AES-256-GCM payload encryption, byte-compatible with PayloadEncryptionCodec.java.

Data = 12-byte nonce + ciphertext + 16-byte tag; the plaintext is the serialized original Payload.
Encodes with the current key; decodes with the current or any previous key, so a key rotation never makes
histories of running or retained workflows unreadable.
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


def _aes(base64_key: str) -> AESGCM:
    key = base64.b64decode(base64_key)
    if len(key) != 32:
        raise ValueError("Payload keys must be 32 bytes, base64 encoded")
    return AESGCM(key)


class EncryptionCodec(PayloadCodec):
    def __init__(self, base64_key: str, key_id: str, previous_keys: str = "") -> None:
        """previous_keys: "id=base64,id=base64" of keys retired from encoding but still needed to decode."""
        self._aes = _aes(base64_key)
        self._key_id = key_id
        self._keys = {key_id: self._aes}
        for entry in filter(str.strip, previous_keys.split(",")):
            old_id, old_key = entry.strip().split("=", 1)
            self._keys[old_id] = _aes(old_key)

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
            aes = self._keys.get(key_id)
            if aes is None:
                raise ValueError(f"Unknown payload key id {key_id}")
            decoded = Payload()
            decoded.ParseFromString(aes.decrypt(p.data[:12], p.data[12:], None))
            out.append(decoded)
        return out
