# Python application (TSA + IS track workers). Build context: repository root.
#   docker build -f python/Dockerfile .                -> runtime image
#   docker build -f python/Dockerfile --target test .  -> runs the Python test suite during the build
FROM python:3.12-slim AS base
WORKDIR /app/python
COPY python/pyproject.toml ./
RUN pip install --no-cache-dir "temporalio==1.34.0" "pydantic>=2.7" "httpx>=0.27" "PyYAML>=6.0" "python-json-logger>=3.2"
COPY python ./
COPY config /app/config
ENV PYTHONUNBUFFERED=1 \
    TRIAGE_RULES=/app/config/triage-rules.yaml

FROM base AS test
RUN pip install --no-cache-dir "pytest>=8" "pytest-asyncio>=0.24"
COPY contracts /app/contracts
# The time-skipping test server is downloaded by the Temporal SDK on first use (needs network).
RUN python -m pytest -q

FROM base AS runtime
CMD ["python", "-m", "claims.temporal.workers.main"]
