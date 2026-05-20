# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy shared library and both services (ops-api imports JobStore from replay-engine)
COPY libs/common /build/libs/common
COPY services/replay-engine /build/services/replay-engine
COPY services/ops-api /build/services/ops-api

# Install in dependency order
RUN pip install --no-cache-dir /build/libs/common
RUN pip install --no-cache-dir /build/services/replay-engine
RUN pip install --no-cache-dir /build/services/ops-api

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

RUN groupadd -r mdrp && useradd -r -g mdrp mdrp

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

USER mdrp

# Main API port
EXPOSE 8080
# Prometheus metrics port
EXPOSE 8007

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" || exit 1

CMD ["uvicorn", "ops_api.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
