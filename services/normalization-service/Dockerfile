# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tooling
RUN pip install --no-cache-dir hatchling pip --upgrade

# Copy and install the shared library first (layer-caching friendly)
COPY libs/common /build/libs/common
RUN pip install --no-cache-dir /build/libs/common

# Copy and install the service
COPY services/normalization-service /build/services/normalization-service
RUN pip install --no-cache-dir /build/services/normalization-service

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Non-root user for least-privilege operation
RUN groupadd --gid 10001 appgroup \
 && useradd  --uid 10001 --gid appgroup --no-create-home appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/normalization-service    /usr/local/bin/normalization-service

USER appuser

# Prometheus metrics port
EXPOSE 8004

# Health-check: ensure the metrics endpoint is reachable
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8004/metrics')"

ENTRYPOINT ["normalization-service"]
