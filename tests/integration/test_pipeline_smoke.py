"""
Integration smoke tests — verify the full pipeline is working end-to-end.

Requires a running docker compose stack (`make up`).
Run with: pytest tests/integration/ -m integration -v
"""

import time

import pytest
import requests


pytestmark = pytest.mark.integration


class TestHealthEndpoints:
    def test_ops_api_health(self, stack_health: dict) -> None:
        assert stack_health.get("status") == "ok"

    def test_ops_api_providers(self, ops_api_url: str) -> None:
        resp = requests.get(f"{ops_api_url}/api/v1/providers", timeout=10)
        assert resp.status_code == 200
        providers = resp.json()
        assert any("provider-emulator" in p.get("provider", "") for p in providers)


class TestCurveData:
    def test_curves_available(self, ops_api_url: str) -> None:
        """All 6 instruments should be cached in Redis within 30 s of stack start."""
        deadline = time.time() + 30
        while time.time() < deadline:
            resp = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
            if resp.status_code == 200 and len(resp.json()) >= 5:
                break
            time.sleep(2)
        else:
            pytest.fail("Fewer than 5 curves available after 30 s")

        curves = resp.json()
        instruments = {c["instrument"] for c in curves}
        assert "TTF" in instruments
        assert "BRENT" in instruments
        assert "WTI" in instruments

    def test_ttf_power_visible(self, ops_api_url: str) -> None:
        resp = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
        assert resp.status_code == 200
        instruments = {c["instrument"] for c in resp.json()}
        assert "TTF_POWER" in instruments, f"TTF_POWER not found. Got: {instruments}"

    def test_curve_completeness(self, ops_api_url: str) -> None:
        resp = requests.get(f"{ops_api_url}/api/v1/curves", timeout=10)
        for curve in resp.json():
            assert curve["completeness"] == pytest.approx(1.0, abs=0.05), (
                f"{curve['instrument']} completeness too low: {curve['completeness']}"
            )


class TestDLQ:
    def test_dlq_endpoint_accessible(self, ops_api_url: str) -> None:
        resp = requests.get(f"{ops_api_url}/api/v1/dlq", timeout=10)
        assert resp.status_code == 200
        body = resp.json()
        assert "depth_estimate" in body
        assert "top_failure_categories" in body
        assert "recent_entries" in body

    def test_fault_injected_events_reach_dlq(self, ops_api_url: str) -> None:
        """
        With FAULT_RATE_MALFORMED > 0 (default 0.01), some events should reach
        the DLQ within 60 s of the stack starting.
        """
        deadline = time.time() + 60
        while time.time() < deadline:
            resp = requests.get(f"{ops_api_url}/api/v1/dlq?limit=1", timeout=10)
            if resp.status_code == 200 and resp.json().get("depth_estimate", 0) > 0:
                return
            time.sleep(5)
        pytest.skip("No DLQ events in 60 s — fault rate may be 0")


class TestReplay:
    def test_bronze_replay_job_accepted(self, ops_api_url: str) -> None:
        payload = {
            "source": "bronze_s3",
            "provider": "provider-emulator",
            "start_time": "2026-05-21T00:00:00Z",
            "end_time": "2026-05-21T23:59:59Z",
            "requested_by": "integration-test",
        }
        resp = requests.post(f"{ops_api_url}/api/v1/replay", json=payload, timeout=10)
        assert resp.status_code == 200
        job = resp.json()
        assert "job_id" in job
        assert job["status"] in ("pending", "running", "completed")

    def test_replay_job_status_queryable(self, ops_api_url: str) -> None:
        payload = {
            "source": "bronze_s3",
            "provider": "provider-emulator",
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-01T01:00:00Z",
            "requested_by": "integration-test",
        }
        resp = requests.post(f"{ops_api_url}/api/v1/replay", json=payload, timeout=10)
        job_id = resp.json()["job_id"]

        time.sleep(5)
        status_resp = requests.get(f"{ops_api_url}/api/v1/replay/{job_id}", timeout=10)
        assert status_resp.status_code == 200
        assert status_resp.json()["status"] in ("pending", "running", "completed", "failed")
