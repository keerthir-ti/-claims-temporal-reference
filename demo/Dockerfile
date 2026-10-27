# Demo tools: the 7 question walkthroughs, scenario runner, operator CLI, load test, Actions estimator.
# Build context: repository root. Uses the Temporal client code from python/claims (names,
# contracts, connection) but nothing from mock-data/: it talks to the mocks only over their demo API.
FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir "temporalio==1.34.0" "pydantic>=2.7" "httpx>=0.27" "PyYAML>=6.0" \
    "python-json-logger>=3.2" "psycopg[binary]>=3.2"
COPY python/claims /app/python/claims
COPY contracts /app/contracts
COPY demo/demo /app/demo
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app/python:/app CONTRACTS_DIR=/app/contracts
CMD ["python", "-m", "demo.cli", "--help"]
