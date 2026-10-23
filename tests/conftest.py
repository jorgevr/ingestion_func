"""Shared pytest fixtures for PVDAQ ingestion function tests."""

from __future__ import annotations

import os

import pytest

# function_app.py validates configuration eagerly at import time (fail fast
# in production — see function_app.py's module-level load_historical_config()
# call). Tests must therefore see a complete, valid environment the moment
# function_app is first imported (module import happens once, at collection
# time, before any individual test's own patch.dict(os.environ, ...) takes
# effect). pytest_configure runs before collection, so this seeds the
# ambient environment early enough. setdefault() so a real shell environment
# (e.g. docker-compose) is never clobbered; individual tests still exercise
# both valid and invalid configs directly via load_historical_config().
_BASELINE_TEST_ENV: dict[str, str] = {
    "SERVICE_BUS_QUEUE_NAME": "raw-energy-events",
    "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
    "TableStorageConnection__tableServiceUri": "https://teststorage.table.core.windows.net",
    "ServiceBusConnection": "Endpoint=sb://test;SharedAccessKeyName=Root;SharedAccessKey=test;UseDevelopmentEmulator=true;",
    "PVDAQ_HISTORICAL_SITE_IDS": "9068",
    "PVDAQ_HISTORICAL_CRON_SCHEDULE": "0 0 * * * *",
    "PVDAQ_HISTORICAL_QUEUE_NAME": "pvdaq-historical-work",
    "FILE_TRACKING_TABLE_NAME": "PvdaqFileTracking",
    "BRONZE_CONTAINER": "bronze",
    "DATA_STORAGE_CONNECTION": "UseDevelopmentStorage=true",
}


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "azurite: requires a live Azurite instance — skipped unless "
        "AZURITE_TEST_CONNECTION is set to its connection string.",
    )
    for key, value in _BASELINE_TEST_ENV.items():
        os.environ.setdefault(key, value)


@pytest.fixture()
def correlation_id() -> str:
    """A fixed UUID string for test reproducibility."""
    return "550e8400-e29b-41d4-a716-446655440000"
