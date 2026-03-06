"""Shared pytest fixtures for PVDAQ ingestion function tests."""

from __future__ import annotations

import pytest

from src.config import Config


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
        service_bus_topic_name="energy-telemetry-ingested",
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
