"""JSON log lines on stdout for ELK, with the same field names as the Java workers."""
from __future__ import annotations

import logging
import os
import sys

from pythonjsonlogger.json import JsonFormatter


def configure(service: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s",
                                       static_fields={"service": os.environ.get("SERVICE_NAME", service)}))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
