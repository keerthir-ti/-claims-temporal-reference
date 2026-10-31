# claim-sim: mock application driver. Build context: claim-orchestrator/.
#   docker build -f tools/claim-sim/Dockerfile .               runtime image

FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install --no-cache-dir "temporalio==1.34.0" "grpcio-tools==1.85.1" "cryptography>=43" \
      "python-json-logger>=3.2" "PyYAML>=6.0" "psycopg[binary]>=3.2"
COPY contracts/claims /app/contracts/claims
RUN mkdir -p /app/gen && python -m grpc_tools.protoc -I/app/contracts --python_out=/app/gen /app/contracts/claims/v1/*.proto
COPY shared/runtime-python/claims_runtime /app/claims_runtime
COPY tools/claim-sim/claim_sim /app/claim_sim
COPY config /app/config
COPY mock-data /app/mock-data
ENV PYTHONPATH=/app/gen:/app PYTHONUNBUFFERED=1


FROM base AS runtime
ENTRYPOINT ["python", "-m", "claim_sim.main"]
CMD ["--help"]
