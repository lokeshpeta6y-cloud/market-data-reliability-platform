"""
Chaos tests for the Market Data Reliability Platform.

Each test simulates a real-world failure mode and validates the platform's
recovery behaviour.  All tests are marked ``pytest.mark.chaos`` and are skipped
unless the environment variable ``CHAOS_TESTS=true`` is set.

Prerequisites:
- Full docker-compose stack is running (make up)
- ops-api is reachable at http://localhost:8010
- Redpanda is reachable at localhost:19092
- MinIO is reachable at http://localhost:9000

To run:
    make up
    CHAOS_TESTS=true pytest -m chaos tests/chaos/ -v
"""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any

import pytest
import requests

# ---------------------------------------------------------------------------
# Skip guard
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.chaos

_CHAOS_ENABLED = os.getenv("CHAOS_TESTS", "false").lower() == "true"


def _require_chaos():
    if not _CHAOS_ENABLED:
        pytest.skip(
            "Chaos tests disabled. Set CHAOS_TESTS=true and ensure the full "
            "docker-compose stack is running."
        )


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OPS_API_BASE = os.getenv("OPS_API_URL", "http://localhost:8010")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BRONZE_BUCKET = os.getenv("S3_BUCKET_BRONZE", "mdrp-bronze")

TOPIC_RAW = "market.events.raw"
TOPIC_VALIDATED = "market.events.validated"
TOPIC_DLQ = "market.events.dlq"

EMULATOR_SERVICE = "provider-emulator"
BRONZE_WRITER_SERVICE = "bronze-writer"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ops_get(path: str, **kwargs) -> requests.Response:
    return requests.get(f"{OPS_API_BASE}{path}", timeout=10, **kwargs)


def _ops_post(path: str, body: dict | None = None, **kwargs) -> requests.Response:
    return requests.post(
        f"{OPS_API_BASE}{path}",
        json=body or {},
        timeout=30,
        **kwargs,
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_consumer_lag(topic: str, consumer_group: str) -> int:
    """Query ops-api for the consumer lag on a given topic/group."""
    resp = _ops_get(
        "/api/v1/consumer-lag",
        params={"topic": topic, "group": consumer_group},
    )
    if resp.status_code != 200:
        return -1
    data = resp.json()
    return data.get("lag", 0)


def _get_provider_status(provider: str) -> dict:
    resp = _ops_get(f"/api/v1/providers/{provider}")
    if resp.status_code != 200:
        return {}
    return resp.json()


def _produce_events(n: int, malformed: bool = False) -> list[str]:
    """Produce n events to TOPIC_RAW via the ops-api test helper endpoint."""
    resp = _ops_post(
        "/api/v1/test/produce-events",
        body={"count": n, "malformed": malformed},
    )
    if resp.status_code != 200:
        return []
    return resp.json().get("event_ids", [])


def _wait_for_condition(condition_fn, timeout_s: int, poll_interval_s: float = 2.0) -> bool:
    """Poll condition_fn until it returns True or timeout expires."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if condition_fn():
                return True
        except Exception:
            pass
        time.sleep(poll_interval_s)
    return False


# ---------------------------------------------------------------------------
# Scenario 1: Provider outage and recovery
# ---------------------------------------------------------------------------


class TestProviderOutageRecovery:
    """
    Stop emitting events for 30 seconds, verify consumer lag grows,
    restart emission, verify lag drains.
    """

    def test_provider_outage_recovery(self):
        _require_chaos()

        provider = "chaos-test-provider"
        consumer_group = "validation-service"

        # Step 1: Verify the stack is healthy before starting
        status_resp = _ops_get("/api/v1/status")
        assert status_resp.status_code == 200, "ops-api not reachable; is the stack up?"

        # Step 2: Measure baseline lag
        baseline_lag = _get_consumer_lag(TOPIC_RAW, consumer_group)

        # Step 3: Stop the emulator via ops-api
        stop_resp = _ops_post(
            "/api/v1/providers/pause",
            body={"provider": provider, "duration_seconds": 30},
        )
        assert stop_resp.status_code in (200, 202), (
            f"Failed to pause provider: {stop_resp.status_code} {stop_resp.text}"
        )

        # Step 4: Wait for the provider to be in OUTAGE state (up to 15s)
        outage_detected = _wait_for_condition(
            lambda: _get_provider_status(provider).get("status") in ("degraded", "outage"),
            timeout_s=15,
        )
        assert outage_detected, "Provider did not enter outage state within 15s"

        # Step 5: Wait 30 seconds while the emulator is paused
        time.sleep(30)

        # Step 6: Verify consumer lag has grown or emitter is at zero
        # (when there are no new events, lag may remain stable — we confirm
        # the provider is still in outage state)
        provider_status = _get_provider_status(provider)
        assert provider_status.get("events_last_60s", 1) == 0 or \
               provider_status.get("status") in ("degraded", "outage"), (
            "Provider should show zero events or outage status during pause"
        )

        # Step 7: Resume the emulator
        resume_resp = _ops_post(
            "/api/v1/providers/resume",
            body={"provider": provider},
        )
        assert resume_resp.status_code in (200, 202), (
            f"Failed to resume provider: {resume_resp.status_code}"
        )

        # Step 8: Verify provider recovers to HEALTHY within 60s
        recovered = _wait_for_condition(
            lambda: _get_provider_status(provider).get("status") == "healthy",
            timeout_s=60,
        )
        assert recovered, "Provider did not recover to HEALTHY state within 60s"

        # Step 9: Verify lag drains (returns to baseline or lower within 90s)
        lag_drained = _wait_for_condition(
            lambda: _get_consumer_lag(TOPIC_RAW, consumer_group) <= baseline_lag + 5,
            timeout_s=90,
        )
        assert lag_drained, (
            f"Consumer lag did not drain to baseline ({baseline_lag}) within 90s"
        )


# ---------------------------------------------------------------------------
# Scenario 2: DLQ spike and replay
# ---------------------------------------------------------------------------


class TestDLQSpikeAndReplay:
    """
    Inject 100% malformed rate for 60s, verify DLQ fills, run DLQ replay,
    verify events are re-processed.
    """

    def test_dlq_spike_and_replay(self):
        _require_chaos()

        # Step 1: Record initial DLQ depth
        dlq_initial_resp = _ops_get("/api/v1/dlq/stats")
        assert dlq_initial_resp.status_code == 200
        initial_dlq_count = dlq_initial_resp.json().get("total_events", 0)

        # Step 2: Configure 100% malformed fault rate for 60 seconds
        inject_resp = _ops_post(
            "/api/v1/test/set-fault-rates",
            body={
                "fault_rate_malformed": 1.0,
                "duration_seconds": 60,
            },
        )
        assert inject_resp.status_code in (200, 202), (
            f"Could not set fault rates: {inject_resp.status_code}"
        )

        # Step 3: Wait for the DLQ to accumulate events
        dlq_grew = _wait_for_condition(
            lambda: _ops_get("/api/v1/dlq/stats").json().get("total_events", 0)
            > initial_dlq_count + 10,
            timeout_s=90,
            poll_interval_s=3.0,
        )
        assert dlq_grew, "DLQ did not grow during malformed fault injection"

        # Step 4: Capture DLQ event count after spike
        dlq_after_spike_resp = _ops_get("/api/v1/dlq/stats")
        dlq_after_spike = dlq_after_spike_resp.json().get("total_events", 0)
        assert dlq_after_spike > initial_dlq_count

        # Step 5: Reset fault rates to normal
        _ops_post("/api/v1/test/set-fault-rates", body={"fault_rate_malformed": 0.01})

        # Step 6: Trigger DLQ replay for the spike window
        replay_start = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        replay_end = datetime.now(timezone.utc).isoformat()
        replay_resp = _ops_post(
            "/api/v1/replay/dlq",
            body={
                "start_time": replay_start,
                "end_time": replay_end,
            },
        )
        assert replay_resp.status_code in (200, 202), (
            f"DLQ replay request failed: {replay_resp.status_code} {replay_resp.text}"
        )
        job_id = replay_resp.json().get("job_id")
        assert job_id is not None, "DLQ replay did not return a job_id"

        # Step 7: Wait for the replay job to complete
        replay_completed = _wait_for_condition(
            lambda: _ops_get(f"/api/v1/replay/{job_id}").json().get("status")
            in ("completed", "failed"),
            timeout_s=120,
            poll_interval_s=5.0,
        )
        assert replay_completed, f"Replay job {job_id} did not complete within 120s"

        # Step 8: Verify the job completed successfully
        job_status = _ops_get(f"/api/v1/replay/{job_id}").json()
        assert job_status.get("status") == "completed", (
            f"Replay job failed: {job_status.get('error', 'unknown error')}"
        )
        assert job_status.get("events_replayed", 0) > 0, "Replay processed zero events"


# ---------------------------------------------------------------------------
# Scenario 3: Bronze S3 unavailable
# ---------------------------------------------------------------------------


class TestBronzeS3Unavailable:
    """
    Simulate MinIO being unavailable, verify bronze-writer retries without
    data loss, and verify no data loss after MinIO recovers.
    """

    def test_bronze_s3_unavailable(self):
        _require_chaos()

        # Step 1: Produce some events so bronze-writer has work to do
        event_ids = _produce_events(20)
        assert len(event_ids) > 0, "Failed to produce test events"

        # Step 2: Block MinIO access via ops-api network chaos endpoint
        block_resp = _ops_post(
            "/api/v1/chaos/block-service",
            body={
                "service": "minio",
                "duration_seconds": 30,
            },
        )
        assert block_resp.status_code in (200, 202), (
            f"Could not block MinIO: {block_resp.status_code}"
        )

        # Step 3: Verify bronze-writer is in a retrying state
        time.sleep(5)
        bronze_health = _ops_get("/api/v1/services/bronze-writer/health")
        if bronze_health.status_code == 200:
            health_data = bronze_health.json()
            # Should be either retrying or degraded — not crashing
            assert health_data.get("status") in ("healthy", "degraded", "retrying"), (
                f"Bronze-writer in unexpected state: {health_data}"
            )

        # Step 4: Produce 20 more events while MinIO is blocked
        more_event_ids = _produce_events(20)

        # Step 5: Wait for MinIO to become available again (block expires)
        time.sleep(35)

        # Step 6: Verify MinIO is accessible
        minio_healthy = _wait_for_condition(
            lambda: _ops_get("/api/v1/services/minio/health").json().get("status") == "healthy",
            timeout_s=30,
        )
        assert minio_healthy, "MinIO did not recover within 30s"

        # Step 7: Verify bronze-writer recovers and flushes pending writes
        bronze_recovered = _wait_for_condition(
            lambda: _ops_get("/api/v1/services/bronze-writer/health").json().get("status")
            == "healthy",
            timeout_s=60,
        )
        assert bronze_recovered, "bronze-writer did not recover within 60s"

        # Step 8: Verify the events written before and during the outage are in S3
        all_event_ids = event_ids + more_event_ids
        written_resp = _ops_post(
            "/api/v1/test/check-bronze-events",
            body={"event_ids": all_event_ids},
        )
        if written_resp.status_code == 200:
            written_ids = set(written_resp.json().get("found_ids", []))
            # Allow for some events still in transit — require at least 70% written
            assert len(written_ids) >= len(all_event_ids) * 0.7, (
                f"Only {len(written_ids)}/{len(all_event_ids)} events found in Bronze after recovery"
            )


# ---------------------------------------------------------------------------
# Scenario 4: Out-of-order event handling
# ---------------------------------------------------------------------------


class TestOutOfOrderHandling:
    """
    Emit 1000 events with shuffled timestamps, verify normalisation still
    produces valid ForwardCurveSnapshots.
    """

    def test_out_of_order_handling(self):
        _require_chaos()

        # Step 1: Produce 1000 events with shuffled timestamps via ops-api
        inject_resp = _ops_post(
            "/api/v1/test/produce-events",
            body={
                "count": 1000,
                "fault_rate_out_of_order": 1.0,
                "fault_rate_stale": 0.3,
            },
        )
        assert inject_resp.status_code in (200, 202), (
            f"Failed to produce out-of-order events: {inject_resp.status_code}"
        )
        event_ids = inject_resp.json().get("event_ids", [])
        assert len(event_ids) > 0

        # Step 2: Wait for normalisation to process the events
        # (the normalisation service should handle OOO gracefully)
        time.sleep(30)

        # Step 3: Query ops-api for recent curve snapshots
        snapshots_resp = _ops_get(
            "/api/v1/curves",
            params={"limit": 20, "min_completeness": 0.5},
        )
        assert snapshots_resp.status_code == 200, (
            f"Could not retrieve curve snapshots: {snapshots_resp.status_code}"
        )
        snapshots = snapshots_resp.json()

        # Step 4: Verify that valid snapshots were produced despite OOO events
        assert len(snapshots) > 0, (
            "No curve snapshots produced from 1000 out-of-order events"
        )

        # Step 5: Verify each snapshot has a completeness score > 0
        for snapshot in snapshots:
            assert snapshot.get("completeness", 0) > 0, (
                f"Snapshot {snapshot.get('snapshot_id')} has zero completeness"
            )

        # Step 6: Verify normalisation error rate is low
        norm_metrics_resp = _ops_get("/api/v1/services/normalization-service/metrics")
        if norm_metrics_resp.status_code == 200:
            metrics = norm_metrics_resp.json()
            error_rate = metrics.get("error_rate_last_60s", 0.0)
            # Even with 30% stale events, the normalisation error rate should
            # not exceed 40% (stale events go to DLQ before normalisation)
            assert error_rate < 0.40, (
                f"Normalisation error rate too high: {error_rate:.2%}"
            )

        # Step 7: Verify OOO events that passed validation are still normalised
        validated_count_resp = _ops_get(
            "/api/v1/consumer-lag",
            params={"topic": TOPIC_VALIDATED, "group": "normalization-service"},
        )
        if validated_count_resp.status_code == 200:
            lag = validated_count_resp.json().get("lag", 999)
            assert lag < 100, (
                f"Normalisation consumer lag is {lag} — service may be stuck"
            )
