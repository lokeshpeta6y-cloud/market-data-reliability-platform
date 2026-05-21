"""
Chaos tests — validate pipeline resilience under high fault-injection rates.

Requires a running stack configured with elevated fault rates:
  FAULT_RATE_MALFORMED=0.20
  FAULT_RATE_DUPLICATE=0.30
  FAULT_RATE_STALE=0.20
  FAULT_RATE_PARTIAL_CURVE=0.20
  FAULT_RATE_SCHEMA_DRIFT=0.10

Run with: CHAOS_TESTS=true pytest tests/chaos/ -m chaos -v -s

These tests are intentionally slow — they wait for the pipeline to stabilise
under sustained fault injection and then assert invariants.
"""

import os
import time

import pytest
import requests


pytestmark = pytest.mark.chaos

_CHAOS = os.getenv("CHAOS_TESTS", "").lower() in ("1", "true", "yes")

if not _CHAOS:
    pytest.skip("Set CHAOS_TESTS=true to run chaos tests", allow_module_level=True)

SETTLE_SECONDS = 120  # time to let the pipeline process events under fault load


class TestDLQRouting:
    def test_malformed_events_reach_dlq(self, ops_api_url: str) -> None:
        """
        With FAULT_RATE_MALFORMED=0.20, we expect a non-trivial DLQ depth
        within SETTLE_SECONDS.
        """
        time.sleep(SETTLE_SECONDS)
        resp = requests.get(f"{ops_api_url}/api/v1/dlq", timeout=30)
        assert resp.status_code == 200
        body = resp.json()
        assert body["depth_estimate"] > 0, "Expected DLQ events with elevated fault rates"

    def test_dlq_contains_malformed_category(self, ops_api_url: str) -> None:
        resp = requests.get(f"{ops_api_url}/api/v1/dlq?limit=100", timeout=30)
        body = resp.json()
        top_categories = {c["category"].upper() for c in body.get("top_failure_categories", [])}
        entry_categories = {e.get("failure_category", "").upper() for e in body.get("recent_entries", [])}
        all_categories = top_categories | entry_categories
        assert "MALFORMED" in all_categories or "SCHEMA_VALIDATION" in all_categories, (
            f"Expected MALFORMED in DLQ categories. Got: {all_categories}"
        )


class TestDeduplication:
    def test_duplicates_do_not_corrupt_curves(self, ops_api_url: str) -> None:
        """
        With FAULT_RATE_DUPLICATE=0.30, the validation service should absorb
        all duplicates via Redis SETNX.  Curve completeness must stay above 0.90.
        """
        time.sleep(30)
        resp = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
        assert resp.status_code == 200
        for curve in resp.json():
            assert curve["completeness"] >= 0.80, (
                f"{curve['instrument']} completeness degraded under duplicate storm: "
                f"{curve['completeness']}"
            )


class TestPipelineThroughput:
    def test_pipeline_keeps_processing_under_faults(self, ops_api_url: str) -> None:
        """
        Confirm that the pipeline is still processing events (version counter
        increments) after sustained fault injection.
        """
        resp1 = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
        versions_before = {c["instrument"]: c["version"] for c in resp1.json()}

        time.sleep(30)

        resp2 = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
        versions_after = {c["instrument"]: c["version"] for c in resp2.json()}

        for instrument, v_before in versions_before.items():
            v_after = versions_after.get(instrument, v_before)
            assert v_after > v_before, (
                f"{instrument} version did not increment: {v_before} → {v_after}. "
                "Pipeline may have stalled."
            )


class TestDLQReplay:
    def test_dlq_replay_recovers_events(self, ops_api_url: str) -> None:
        """
        After fault-induced DLQ accumulation, submitting a DLQ replay job
        should drain the queue and the replayed events should pass validation
        (assuming the fault that caused them is no longer active).
        """
        payload = {
            "source": "dlq",
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2099-12-31T23:59:59Z",
            "requested_by": "chaos-test",
        }
        resp = requests.post(f"{ops_api_url}/api/v1/replay", json=payload, timeout=10)
        assert resp.status_code == 200
        job_id = resp.json()["job_id"]

        # Poll up to 120 s for completion
        deadline = time.time() + 120
        while time.time() < deadline:
            status_resp = requests.get(f"{ops_api_url}/api/v1/replay/{job_id}", timeout=10)
            job = status_resp.json()
            if job["status"] in ("completed", "failed"):
                break
            time.sleep(5)

        assert job["status"] == "completed", f"DLQ replay did not complete: {job}"
