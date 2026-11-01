import base64

import pytest
from temporalio.api.common.v1 import Payload

from claims_runtime.codec import EncryptionCodec

KEY_1 = base64.b64encode(bytes(32)).decode()
KEY_2 = base64.b64encode(b"0123456789abcdef0123456789abcdef").decode()
PLAIN = Payload(metadata={"encoding": b"json/protobuf"}, data=b'{"claimId":"C1"}')


async def test_after_rotation_old_histories_still_decode():
    [sealed] = await EncryptionCodec(KEY_1, "key-1").encode([PLAIN])
    rotated = EncryptionCodec(KEY_2, "key-2", f"key-1={KEY_1}")
    assert await rotated.decode([sealed]) == [PLAIN]
    [resealed] = await rotated.encode([PLAIN])
    assert resealed.metadata["encryption-key-id"] == b"key-2"


async def test_unknown_key_is_refused():
    [sealed] = await EncryptionCodec(KEY_1, "key-1").encode([PLAIN])
    with pytest.raises(ValueError):
        await EncryptionCodec(KEY_2, "key-2").decode([sealed])
