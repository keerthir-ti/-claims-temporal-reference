# Investigation (IS) worker (Python). Build context: claim-orchestrator/.
#   docker build -f workers/investigation-worker/Dockerfile .               runtime image
#   docker build -f workers/investigation-worker/Dockerfile --target test .  runs the tests
FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install --no-cache-dir "temporalio==1.34.0" "grpcio-tools==1.85.1" "cryptography>=43" \
      "python-json-logger>=3.2" "PyYAML>=6.0" "psycopg[binary]>=3.2"
COPY contracts/claims /app/contracts/claims
RUN mkdir -p /app/gen && python -m grpc_tools.protoc -I/app/contracts --python_out=/app/gen /app/contracts/claims/*.proto
COPY workers/ai-worker-common/claims_runtime /app/claims_runtime
COPY workers/investigation-worker/investigation_worker /app/investigation_worker
COPY config /app/config
ENV PYTHONPATH=/app/gen:/app PYTHONUNBUFFERED=1

FROM base AS test
RUN pip install --no-cache-dir "pytest>=8" "pytest-asyncio>=0.24"
COPY workers/investigation-worker/tests /app/tests
# The SDK downloads a local Temporal dev server on first use.
RUN python -m pytest -q -o asyncio_mode=auto -o asyncio_default_fixture_loop_scope=function /app/tests

FROM base AS runtime
CMD ["python", "-m", "investigation_worker.main"]
