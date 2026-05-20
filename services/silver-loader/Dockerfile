# =============================================================================
# Stage 1: builder
# Installs all Python dependencies into a virtual environment.
# The builder image is discarded after the final stage copies the venv.
# =============================================================================
FROM python:3.11-slim AS builder

# System deps needed to build C-extension wheels (confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Create a virtual environment that will be copied into the final image
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the shared library source so it can be installed as a path dependency
COPY libs/common /build/libs/common

# Copy the service pyproject.toml and source
COPY services/silver-loader/pyproject.toml /build/services/silver-loader/
COPY services/silver-loader/src /build/services/silver-loader/src

# Install mdrp-common first (referenced as a path dep in pyproject.toml)
RUN pip install --upgrade pip wheel && \
    pip install --no-cache-dir /build/libs/common

# Install the service with the optional Snowflake connector
RUN pip install --no-cache-dir "/build/services/silver-loader[snowflake]"


# =============================================================================
# Stage 2: runtime
# Lean final image with no build tools.
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="silver-loader" \
      org.opencontainers.image.description="MDRP Silver layer Snowflake loader" \
      org.opencontainers.image.version="0.1.0"

# Runtime system libs required by confluent-kafka and snowflake-connector
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

COPY --chown=appuser:appgroup services/silver-loader/src /app/src

USER appuser

# Prometheus metrics endpoint
EXPOSE 8008

# Health check — the Prometheus metrics server doubles as a liveness probe
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8008/metrics | grep -q "mdrp_service_info" || exit 1

# Environment variable defaults (all overridable at runtime)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    METRICS_PORT=8008 \
    KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    KAFKA_CONSUMER_GROUP=silver-loader \
    BATCH_SIZE=1000 \
    FLUSH_INTERVAL_SECONDS=60 \
    SNOWFLAKE_STAGE_NAME=MDRP_STAGE \
    SNOWFLAKE_DATABASE=MARKET_DATA_PLATFORM \
    SNOWFLAKE_SCHEMA_SILVER=SILVER_EVENTS \
    SNOWFLAKE_WAREHOUSE=INGESTION_WH

ENTRYPOINT ["silver-loader"]
