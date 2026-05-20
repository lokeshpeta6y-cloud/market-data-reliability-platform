# =============================================================================
# Stage 1: builder
# Installs all Python dependencies into a virtual environment.
# =============================================================================
FROM python:3.11-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libssl-dev \
        libffi-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy the shared library source
COPY libs/common /build/libs/common

# Copy the service pyproject.toml and source
COPY services/gold-loader/pyproject.toml /build/services/gold-loader/
COPY services/gold-loader/src /build/services/gold-loader/src

# Install mdrp-common first (referenced as a path dep)
RUN pip install --upgrade pip wheel && \
    pip install --no-cache-dir /build/libs/common

# Install the service with the optional Snowflake connector
RUN pip install --no-cache-dir "/build/services/gold-loader[snowflake]"


# =============================================================================
# Stage 2: runtime
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="gold-loader" \
      org.opencontainers.image.description="MDRP Gold layer Snowflake snapshot loader" \
      org.opencontainers.image.version="0.1.0"

RUN apt-get update && apt-get install -y --no-install-recommends \
        libssl3 \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN groupadd --gid 10001 appgroup && \
    useradd --uid 10001 --gid appgroup --no-create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY --chown=appuser:appgroup services/gold-loader/src /app/src

USER appuser

# Prometheus metrics endpoint
EXPOSE 8009

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8009/metrics | grep -q "mdrp_service_info" || exit 1

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    METRICS_PORT=8009 \
    KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    KAFKA_CONSUMER_GROUP=gold-loader \
    SNAPSHOT_WINDOW_MINUTES=5 \
    MIN_COMPLETENESS=0.80 \
    MIN_QUALITY_SCORE=0.70 \
    EXPECTED_TENORS_PER_CURVE=0 \
    POLL_INTERVAL_SECONDS=5 \
    SNOWFLAKE_STAGE_NAME=MDRP_STAGE \
    SNOWFLAKE_DATABASE=MARKET_DATA_PLATFORM \
    SNOWFLAKE_SCHEMA_GOLD=GOLD_CURVES \
    SNOWFLAKE_WAREHOUSE=INGESTION_WH

ENTRYPOINT ["gold-loader"]
