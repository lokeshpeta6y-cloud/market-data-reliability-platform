# =============================================================================
# Stage 1: builder
# Installs all Python dependencies into a virtual environment.
# The builder image is discarded after the final stage copies the venv.
# =============================================================================
FROM python:3.11-slim AS builder

# System deps needed to build C-extension wheels (confluent-kafka, pyarrow)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Create a virtual environment that we will copy into the final image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the shared library source so it can be installed as a path dependency
COPY libs/common /build/libs/common

# Copy the service pyproject.toml and source
COPY services/bronze-writer/pyproject.toml /build/services/bronze-writer/
COPY services/bronze-writer/src /build/services/bronze-writer/src

# Install mdrp-common first (the path dep in pyproject.toml references it)
RUN pip install --upgrade pip wheel && \
    pip install --no-cache-dir /build/libs/common

# Install the service and its runtime deps
RUN pip install --no-cache-dir /build/services/bronze-writer


# =============================================================================
# Stage 2: runtime
# Lean final image with no build tools.
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="bronze-writer" \
      org.opencontainers.image.description="MDRP Bronze layer Parquet writer" \
      org.opencontainers.image.version="0.1.0"

# Runtime system libs required by confluent-kafka
RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Copy virtual environment from builder
COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Non-root user for security
RUN groupadd --gid 10001 appgroup && \
    useradd --uid 10001 --gid appgroup --no-create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

# Copy source into the image (venv already has the installed package, but having
# the source available aids debugging; omit if image size is a concern)
COPY --chown=appuser:appgroup services/bronze-writer/src /app/src

USER appuser

# Prometheus metrics endpoint
EXPOSE 8003

# Health check — the metrics server doubles as a liveness probe
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8003/metrics | grep -q "mdrp_service_info" || exit 1

# Environment variable defaults (all overridable at runtime)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    METRICS_PORT=8003 \
    KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    KAFKA_CONSUMER_GROUP=bronze-writer \
    S3_BUCKET_BRONZE=mdrp-bronze \
    BATCH_SIZE=500 \
    FLUSH_INTERVAL_SECONDS=30

ENTRYPOINT ["bronze-writer"]
