"""
Replay Engine entrypoint.

Starts the Prometheus metrics server, configures structured logging,
then runs the ReplayEngine polling loop until the process is terminated.
"""

from __future__ import annotations

import sys

from mdrp_common.logging import configure_logging, get_logger
from mdrp_common.metrics import register_metrics

from .engine import ReplayEngine
from .settings import ReplayEngineSettings

_SERVICE_NAME = "replay-engine"


def main() -> None:
    settings = ReplayEngineSettings()

    configure_logging(service_name=_SERVICE_NAME, level=settings.log_level)
    log = get_logger(_SERVICE_NAME)

    register_metrics(
        service_name=_SERVICE_NAME,
        port=settings.metrics_port,
        version=settings.service_version,
    )
    log.info(
        "replay_engine_metrics_started",
        port=settings.metrics_port,
    )

    engine = ReplayEngine(settings=settings)
    try:
        engine.start()
    except Exception as exc:
        log.error("replay_engine_fatal_error", error=str(exc), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
