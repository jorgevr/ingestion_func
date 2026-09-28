"""Shared pytest fixtures for PVDAQ ingestion function tests."""

from __future__ import annotations

import os

import pytest

from src.config import Config

# function_app.py validates configuration eagerly at import time (fail fast
# in production — see function_app.py's module-level load_config() /
# load_historical_config() calls). Tests must therefore see a complete,
# valid environment the moment function_app is first imported (module
# import happens once, at collection time, before any individual test's
# own patch.dict(os.environ, ...) takes effect). pytest_configure runs
# before collection, so this seeds the ambient environment early enough.
# setdefault() so a real shell environment (e.g. docker-compose) is never
# clobbered; individual tests still exercise both valid and invalid
# configs directly via load_config()/load_historical_config().
_BASELINE_TEST_ENV: dict[str, str] = {
    "PVDAQ_LOOKBACK_HOURS": "24",
    "PVDAQ_CRON_SCHEDULE": "0 0 * * * *",
    "SERVICE_BUS_QUEUE_NAME": "raw-energy-events",
    "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
    "IDEMPOTENCY_TABLE_NAME": "PvdaqIdempotency",
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
    for key, value in _BASELINE_TEST_ENV.items():
        os.environ.setdefault(key, value)


@pytest.fixture()
def sample_valid_record() -> dict:
    """A valid PVDAQ telemetry record with representative values."""
    return {
        "SiteID": 2,
        "measdatetime": "2026-01-15T12:00:00",
        "ac_power": 4500.0,
        "dc_power": 4800.0,
        "poa_irradiance": 950.0,
        "ambient_temp": 25.3,
        "module_temp": 38.7,
        "wind_speed": 3.2,
        "inverter_efficiency": 96.5,
    }


@pytest.fixture()
def sample_invalid_record() -> dict:
    """An invalid PVDAQ record missing the required SiteID field."""
    return {
        "measdatetime": "2026-01-15T12:00:00",
        "ac_power": 4500.0,
    }


@pytest.fixture()
def mock_config() -> Config:
    """A Config object populated with test-friendly values."""
    return Config(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_systems_key="pvdaq/csv/systems_20250729.csv",
        oedi_data_prefix="pvdaq/csv/pvdata",
        pvdaq_site_ids=[2, 34],
        pvdaq_site_count=30,
        pvdaq_lookback_hours=24,
        pvdaq_cron_schedule="0 */15 * * * *",
        service_bus_queue_name="energy-telemetry-ingested",
        dead_letter_queue_name="pvdaq-dead-letter",
        service_bus_fully_qualified_namespace="test-sb.servicebus.windows.net",
        idempotency_table_name="PvdaqIdempotency",
        table_storage_uri="https://teststorage.table.core.windows.net",
        tenant_id="research",
        mapping_version_pvdaq="unknown",
        schema_version_pvdaq="v1",
    )


@pytest.fixture()
def correlation_id() -> str:
    """A fixed UUID string for test reproducibility."""
    return "550e8400-e29b-41d4-a716-446655440000"
