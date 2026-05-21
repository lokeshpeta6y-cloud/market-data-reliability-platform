"""
Provider Emulator — entry point.

Startup sequence:
1. Load settings from environment / .env file.
2. Configure structured logging.
3. Start the Prometheus metrics HTTP server.
4. Ensure all Redpanda topics exist (idempotent).
5. Create and run the ProviderEmulator.

Exit codes:
  0 — clean shutdown (SIGTERM / SIGINT received)
  1 — fatal startup error (bad config, Kafka unreachable, etc.)
"""

from __future__ import annotations

import sys

from mdrp_common.kafka_client import ensure_topics
from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import register_metrics
from provider_emulator.emulator import ProviderEmulator
from provider_emulator.settings import EmulatorSettings


def main() -> None:
    # ------------------------------------------------------------------ #
    # 1. Load settings
    # ------------------------------------------------------------------ #
    try:
        settings = EmulatorSettings()
    except Exception as exc:
        # Logging is not yet configured — print to stderr and abort.
        print(f"FATAL: failed to load settings: {exc}", file=sys.stderr)
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 2. Configure structured logging
    # ------------------------------------------------------------------ #
    configure_logging(service_name="provider-emulator", level=settings.log_level)
    logger = get_logger(__name__)
    logger.info(
        "provider_emulator_starting",
        version=settings.service_version,
        instruments=settings.instruments,
        publish_interval_seconds=settings.publish_interval_seconds,
        databento_configured=settings.databento_configured,
    )

    # ------------------------------------------------------------------ #
    # 3. Start Prometheus metrics server
    # ------------------------------------------------------------------ #
    try:
        register_metrics(
            service_name="provider-emulator",
            port=settings.metrics_port,
            version=settings.service_version,
        )
        logger.info(
            "metrics_server_started",
            port=settings.metrics_port,
        )
    except OSError as exc:
        logger.error(
            "metrics_server_failed_to_start",
            port=settings.metrics_port,
            error=str(exc),
        )
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 4. Ensure Kafka / Redpanda topics exist
    # ------------------------------------------------------------------ #
    try:
        ensure_topics(bootstrap_servers=settings.kafka_bootstrap_servers)
        logger.info(
            "topics_ensured",
            bootstrap_servers=settings.kafka_bootstrap_servers,
        )
    except Exception as exc:
        logger.error(
            "topic_provisioning_failed",
            bootstrap_servers=settings.kafka_bootstrap_servers,
            error=str(exc),
        )
        sys.exit(1)

    # ------------------------------------------------------------------ #
    # 5. Run the emulator
    # ------------------------------------------------------------------ #
    try:
        emulator = ProviderEmulator(settings=settings)
        emulator.run()
    except Exception as exc:
        logger.exception("emulator_fatal_error", error=str(exc))
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
