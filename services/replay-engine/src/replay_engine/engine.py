"""
Replay Engine — main polling loop.

Polls Redis for pending ReplayJob records, dispatches to the appropriate
replayer, updates job state, and exposes Prometheus metrics.

Shutdown
--------
The engine handles SIGTERM gracefully: it finishes the currently running job
(if any) before exiting so we don't leave jobs stuck in ``running`` state.
"""

from __future__ import annotations

import signal
import threading
import time
from types import FrameType

import redis

from mdrp_common.kafka_client import MdrpProducer, ensure_topics
from mdrp_common.logging import get_logger
from mdrp_common.metrics import REPLAY_DURATION_SECONDS, REPLAY_JOBS_TOTAL
from mdrp_common.models import ReplayJob, ReplaySource
from mdrp_common.storage import BronzeStorageClient
from .bronze_replayer import BronzeReplayer
from .databento_replayer import DatabentoReplayer, _DATABENTO_AVAILABLE
from .dlq_replayer import DLQReplayer
from .job_store import JobStore
from .settings import ReplayEngineSettings

log = get_logger(__name__)


class ReplayEngine:
    """
    Orchestrates replay job execution.

    The engine runs a single-threaded polling loop.  Only one job runs at a
    time — this keeps resource consumption predictable and avoids Kafka
    producer contention.  If higher throughput is needed, deploy multiple
    engine instances; each uses ZPOPMIN for atomic job claiming.
    """

    def __init__(self, settings: ReplayEngineSettings) -> None:
        self._settings = settings
        self._shutdown_event = threading.Event()

        # Redis
        self._redis = redis.from_url(
            settings.redis_url,
            decode_responses=False,
            socket_timeout=5,
            socket_connect_timeout=5,
        )
        self._job_store = JobStore(self._redis)

        # Kafka producer
        self._producer = MdrpProducer(settings.kafka_bootstrap_servers)

        # Storage client for Bronze replayer
        self._storage = BronzeStorageClient(
            bucket=settings.s3_bucket_bronze,
            region=settings.aws_region,
            endpoint_url=settings.s3_endpoint_url,
            aws_access_key_id=settings.aws_access_key_id,
            aws_secret_access_key=settings.aws_secret_access_key,
        )

        # Replayers
        self._bronze_replayer = BronzeReplayer(
            storage_client=self._storage,
            rate_limit_per_second=settings.replay_rate_limit_per_second,
        )

        self._dlq_replayer = DLQReplayer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            consumer_group=settings.dlq_replay_consumer_group,
            rate_limit_per_second=settings.replay_rate_limit_per_second,
            max_messages=settings.dlq_replay_max_messages,
        )

        self._databento_replayer: DatabentoReplayer | None = None
        if settings.databento_configured and _DATABENTO_AVAILABLE:
            self._databento_replayer = DatabentoReplayer(
                api_key=settings.databento_api_key,  # type: ignore[arg-type]
                dataset=settings.databento_dataset,
                rate_limit_per_second=settings.replay_rate_limit_per_second,
            )
            log.info("databento_replayer_enabled", dataset=settings.databento_dataset)
        else:
            log.info(
                "databento_replayer_disabled",
                api_key_set=settings.databento_configured,
                sdk_installed=_DATABENTO_AVAILABLE,
            )

        # Register SIGTERM handler
        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """
        Ensure Kafka topics exist and begin the polling loop.
        This is a blocking call — it returns only after shutdown.
        """
        log.info("replay_engine_starting")
        ensure_topics(self._settings.kafka_bootstrap_servers)
        self._poll_loop()

    def stop(self) -> None:
        """Request a graceful shutdown."""
        log.info("replay_engine_stop_requested")
        self._shutdown_event.set()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _poll_loop(self) -> None:
        """Continuously poll for pending jobs until shutdown is requested."""
        log.info(
            "replay_engine_poll_loop_started",
            poll_interval_s=self._settings.job_poll_interval_seconds,
        )

        while not self._shutdown_event.is_set():
            try:
                job = self._job_store.claim_pending_job()
            except redis.RedisError as exc:
                log.error("redis_error_claiming_job", error=str(exc))
                self._shutdown_event.wait(timeout=self._settings.job_poll_interval_seconds)
                continue

            if job is None:
                self._shutdown_event.wait(timeout=self._settings.job_poll_interval_seconds)
                continue

            self._execute_job(job)

        log.info("replay_engine_poll_loop_exited")
        # Flush any buffered Kafka messages before the process exits
        self._producer.flush(timeout_s=15.0)

    def _execute_job(self, job: ReplayJob) -> None:
        """Dispatch a job to the correct replayer and update its state."""
        log.info(
            "replay_job_starting",
            job_id=job.job_id,
            source=job.source,
            provider=job.provider,
            instrument=job.instrument,
        )

        start_mono = time.monotonic()
        try:
            events_replayed = self._dispatch(job)
            duration = time.monotonic() - start_mono

            self._job_store.update_status(
                job_id=job.job_id,
                status="completed",
                events_replayed=events_replayed,
            )
            REPLAY_JOBS_TOTAL.labels(source=job.source.value, outcome="completed").inc()
            REPLAY_DURATION_SECONDS.labels(source=job.source.value).observe(duration)

            log.info(
                "replay_job_completed",
                job_id=job.job_id,
                source=job.source,
                events_replayed=events_replayed,
                duration_s=round(duration, 2),
            )

        except Exception as exc:
            duration = time.monotonic() - start_mono
            error_msg = str(exc)

            self._job_store.update_status(
                job_id=job.job_id,
                status="failed",
                error=error_msg,
            )
            REPLAY_JOBS_TOTAL.labels(source=job.source.value, outcome="failed").inc()
            REPLAY_DURATION_SECONDS.labels(source=job.source.value).observe(duration)

            log.error(
                "replay_job_failed",
                job_id=job.job_id,
                source=job.source,
                error=error_msg,
                duration_s=round(duration, 2),
            )

    def _dispatch(self, job: ReplayJob) -> int:
        """Route the job to the correct replayer, returning events replayed."""
        if job.source == ReplaySource.BRONZE_S3:
            return self._bronze_replayer.replay(job, self._producer)

        if job.source == ReplaySource.DLQ:
            return self._dlq_replayer.replay(job, self._producer)

        if job.source == ReplaySource.DATABENTO_HISTORICAL:
            if self._databento_replayer is None:
                raise RuntimeError(
                    "Databento replayer is not available. "
                    "Ensure DATABENTO_API_KEY is set and the databento package is installed."
                )
            return self._databento_replayer.replay(job, self._producer)

        raise ValueError(f"unknown replay source: {job.source!r}")

    # ------------------------------------------------------------------
    # Signal handling
    # ------------------------------------------------------------------

    def _handle_sigterm(self, signum: int, frame: FrameType | None) -> None:
        log.info("signal_received_initiating_graceful_shutdown", signal=signum)
        self.stop()
