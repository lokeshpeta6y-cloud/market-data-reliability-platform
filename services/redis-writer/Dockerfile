# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

RUN pip install --no-cache-dir hatchling pip --upgrade

# Shared library
COPY libs/common /build/libs/common
RUN pip install --no-cache-dir /build/libs/common

# Service
COPY services/redis-writer /build/services/redis-writer
RUN pip install --no-cache-dir /build/services/redis-writer

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

RUN groupadd --gid 10001 appgroup \
 && useradd  --uid 10001 --gid appgroup --no-create-home appuser

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin/redis-writer             /usr/local/bin/redis-writer

USER appuser

# Prometheus metrics port
EXPOSE 8005

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8005/metrics')"

ENTRYPOINT ["redis-writer"]
