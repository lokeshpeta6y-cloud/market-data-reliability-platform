# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy shared library first (better layer caching)
COPY libs/common /build/libs/common

# Copy service source
COPY services/replay-engine /build/services/replay-engine

# Install shared lib then service
RUN pip install --no-cache-dir /build/libs/common
RUN pip install --no-cache-dir /build/services/replay-engine

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Non-root user for security
RUN groupadd -r mdrp && useradd -r -g mdrp mdrp

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Health-check script
COPY services/replay-engine/docker-healthcheck.sh /usr/local/bin/healthcheck.sh
RUN chmod +x /usr/local/bin/healthcheck.sh || true

USER mdrp

# Prometheus metrics
EXPOSE 8006

# Graceful shutdown — SIGTERM triggers the engine's shutdown event
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import redis, os; r=redis.from_url(os.environ.get('REDIS_URL','redis://localhost:6379/0')); r.ping()" || exit 1

CMD ["replay-engine"]
