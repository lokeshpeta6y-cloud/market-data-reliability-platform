# =============================================================================
# Stage 1: builder
# Installs all Python dependencies into a virtual environment.
# The builder image is discarded after the final stage copies the venv.
# =============================================================================
FROM python:3.11-slim AS builder

# System deps needed to build C-extension wheels (confluent-kafka, cryptography)
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
COPY services/provider-emulator/pyproject.toml /build/services/provider-emulator/
COPY services/provider-emulator/src /build/services/provider-emulator/src

# Install mdrp-common first (the path dep in pyproject.toml references it)
RUN pip install --upgrade pip wheel && \
    pip install --no-cache-dir /build/libs/common

# Install the service itself plus its non-optional runtime deps.
# We omit the [databento] extra by default — add it via build arg if needed.
ARG INSTALL_DATABENTO=false
RUN if [ "$INSTALL_DATABENTO" = "true" ]; then \
        pip install --no-cache-dir "/build/services/provider-emulator[databento]"; \
    else \
        pip install --no-cache-dir /build/services/provider-emulator; \
    fi


# =============================================================================
# Stage 2: runtime
# Lean final image with no build tools.
# =============================================================================
FROM python:3.11-slim AS runtime

LABEL org.opencontainers.image.title="provider-emulator" \
      org.opencontainers.image.description="MDRP synthetic market data producer" \
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
COPY --chown=appuser:appgroup services/provider-emulator/src /app/src

USER appuser

# Prometheus metrics endpoint
EXPOSE 8001

# Health check — the metrics server doubles as a liveness probe
HEALTHCHECK --interval=15s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -f http://localhost:8001/metrics | grep -q "mdrp_service_info" || exit 1

# Environment variable defaults (all overridable at runtime)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    METRICS_PORT=8001 \
    KAFKA_BOOTSTRAP_SERVERS=redpanda:9092 \
    PUBLISH_INTERVAL_SECONDS=5 \
    FAULT_RATE_DUPLICATE=0.02 \
    FAULT_RATE_MALFORMED=0.01 \
    FAULT_RATE_DELAYED=0.05 \
    FAULT_RATE_OUT_OF_ORDER=0.03 \
    FAULT_RATE_SCHEMA_DRIFT=0.005 \
    FAULT_RATE_STALE=0.01 \
    FAULT_RATE_PARTIAL_CURVE=0.02

ENTRYPOINT ["provider-emulator"]
