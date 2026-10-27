# Fake external systems (ClaimsPro, Policy Gate, Comm Hub, eClaims, ImageRight, AI) + change feed.
# Build context: mock/. stdlib only, so the image needs no dependencies.
FROM python:3.12-slim
WORKDIR /app
COPY mocks ./mocks
COPY fixtures ./fixtures
ENV PYTHONUNBUFFERED=1 FIXTURES_DIR=/app/fixtures/claims
CMD ["python", "-m", "mocks.server"]
