"""One process hosting every fake external system, each under its own base path so each can be
replaced by the real system independently (point that system's *_URL at the real service):

    /claimspro   ClaimsPro      (CLAIMSPRO_URL)      /eclaims     eClaims     (ECLAIMS_URL)
    /policygate  Policy Gate    (POLICYGATE_URL)     /imageright  ImageRight  (IMAGERIGHT_URL)
    /commhub     Comm Hub       (COMMHUB_URL)        /ai          AI models   (AI_URL)
    /demo        demo control (not a mocked system)

stdlib only; run:  python -m mocks.server
"""
from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from mocks import demo_control
from mocks.systems import ai, claimspro, commhub, eclaims, imageright, policygate
from mocks.world import FAILURE_RATE, HttpError

log = logging.getLogger("mocks")

SYSTEMS = {
    "claimspro": claimspro.handle, "policygate": policygate.handle, "commhub": commhub.handle,
    "eclaims": eclaims.handle, "imageright": imageright.handle, "ai": ai.handle, "demo": demo_control.handle,
}


def route(method: str, path: str, body: dict, headers) -> tuple[int, dict]:
    if path == "/health":
        return 200, {"ok": True, "systems": sorted(SYSTEMS)}
    prefix, _, rest = path.lstrip("/").partition("/")
    handler = SYSTEMS.get(prefix)
    if handler is None:
        raise HttpError(404, f"unknown system {prefix!r}")
    return handler(method, "/" + rest, body, headers)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _handle(self, method: str):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            body = json.loads(self.rfile.read(length) or b"{}") if length else {}
            status, payload = route(method, self.path.split("?")[0], body, self.headers)
        except HttpError as e:
            status, payload = e.status, {"error": e.message}
        except Exception as e:  # noqa: BLE001
            log.exception("mock error")
            status, payload = 500, {"error": str(e)}
        data = json.dumps(payload, default=list).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._handle("GET")

    def do_POST(self):
        self._handle("POST")

    def do_PATCH(self):
        self._handle("PATCH")

    def log_message(self, fmt, *args):
        log.debug(fmt, *args)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    port = int(os.environ.get("PORT", "8090"))
    log.info("mock systems on :%d %s (failureRate=%s)", port, sorted(SYSTEMS), FAILURE_RATE)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
