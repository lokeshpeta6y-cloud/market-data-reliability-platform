"""
Shared pytest fixtures and configuration.

Unit tests: no external dependencies — run anywhere.
Integration tests: require a running docker compose stack (`make up`).
Chaos tests: require a running stack with elevated fault injection rates.

Mark tests with @pytest.mark.integration or @pytest.mark.chaos so they are
excluded from plain `pytest tests/unit/` runs.
"""

import os

import pytest
import requests


# ---------------------------------------------------------------------------
# Custom marks — registered here so pytest doesn't warn about unknown marks
# ---------------------------------------------------------------------------

def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: fast, no external dependencies")
    config.addinivalue_line("markers", "integration: requires running docker compose stack")
    config.addinivalue_line("markers", "chaos: requires running stack with elevated fault rates")


# ---------------------------------------------------------------------------
# Stack connectivity fixtures
# ---------------------------------------------------------------------------

OPS_API_URL = os.getenv("OPS_API_URL", "http://localhost:8000")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture(scope="session")
def ops_api_url() -> str:
    return OPS_API_URL


@pytest.fixture(scope="session")
def stack_health(ops_api_url: str) -> dict:
    """
    Session-scoped fixture that verifies the ops-api is reachable.
    Skips the test if the stack is not running.
    """
    try:
        resp = requests.get(f"{ops_api_url}/health", timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        pytest.skip(f"Stack not reachable at {ops_api_url}: {exc}")
