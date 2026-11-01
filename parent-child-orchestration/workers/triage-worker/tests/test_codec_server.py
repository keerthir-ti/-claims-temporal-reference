import base64
import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

from google.protobuf import json_format
from temporalio.api.common.v1 import Payload, Payloads

from claims_runtime.codec import EncryptionCodec
from claims_runtime.codec_server import handler

KEY = base64.b64encode(bytes(32)).decode()
PLAIN = Payload(metadata={"encoding": b"json/protobuf"}, data=b'{"claimId":"C1"}')


def _post(port: int, path: str, payloads: list[Payload]) -> list[Payload]:
    body = json_format.MessageToJson(Payloads(payloads=payloads)).encode()
    request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=body,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request) as response:
        return list(json_format.Parse(response.read(), Payloads()).payloads)


async def test_codec_server_decodes_what_the_workers_encrypt():
    codec = EncryptionCodec(KEY, "key-1")
    [sealed] = await codec.encode([PLAIN])
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler(codec))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        port = server.server_address[1]
        assert _post(port, "/decode", [sealed]) == [PLAIN]
        [encoded] = _post(port, "/encode", [PLAIN])
        assert encoded.metadata["encoding"] == b"binary/encrypted"
    finally:
        server.shutdown()
