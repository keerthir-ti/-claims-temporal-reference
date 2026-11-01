"""Codec server: decodes encrypted payloads for the Temporal UI and CLI, with the same codec as the workers.

Runs in the client's account behind the SSO proxy, so only authorised users reach it; it holds the payload keys and
Temporal never sees them. Implements Temporal's codec server protocol: POST /decode and /encode with a protobuf JSON
Payloads body.

PAYLOAD_KEY, PAYLOAD_KEY_ID, PAYLOAD_PREVIOUS_KEYS  as for the workers
CODEC_SERVER_PORT                                  default 8081
CODEC_CORS_ORIGIN                                  the Temporal UI origin allowed to call it
"""
from __future__ import annotations

import asyncio
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from google.protobuf import json_format
from temporalio.api.common.v1 import Payloads

from claims_runtime.codec import EncryptionCodec


def handler(codec: EncryptionCodec, cors_origin: str = "") -> type[BaseHTTPRequestHandler]:
    class CodecHandler(BaseHTTPRequestHandler):
        def do_OPTIONS(self) -> None:
            self.send_response(204)
            self._cors()
            self.end_headers()

        def do_POST(self) -> None:
            if self.path not in ("/encode", "/decode"):
                self.send_error(404)
                return
            try:
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                payloads = list(json_format.Parse(body, Payloads()).payloads)
                convert = codec.encode if self.path == "/encode" else codec.decode
                reply = json_format.MessageToJson(Payloads(payloads=asyncio.run(convert(payloads)))).encode()
            except Exception as e:  # a payload this server cannot read: say so, never crash
                self.send_error(400, str(e))
                return
            self.send_response(200)
            self._cors()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

        def _cors(self) -> None:
            if cors_origin:
                self.send_header("Access-Control-Allow-Origin", cors_origin)
                self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "content-type, x-namespace, authorization")
                self.send_header("Access-Control-Allow-Credentials", "true")

        def log_message(self, *args) -> None:  # payloads are never logged
            pass

    return CodecHandler


def main() -> None:
    codec = EncryptionCodec(os.environ["PAYLOAD_KEY"], os.environ.get("PAYLOAD_KEY_ID", "key-1"),
                            os.environ.get("PAYLOAD_PREVIOUS_KEYS", ""))
    port = int(os.environ.get("CODEC_SERVER_PORT", "8081"))
    ThreadingHTTPServer(("0.0.0.0", port), handler(codec, os.environ.get("CODEC_CORS_ORIGIN", ""))).serve_forever()


if __name__ == "__main__":
    main()
