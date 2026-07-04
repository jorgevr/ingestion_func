"""Externalized configuration loader for PVDAQ ingestion functions.

Reads all settings from os.environ, parses typed values, and raises
ConfigurationError on missing required settings.

Provides two config loaders:
- load_config(): Feature 001 — daily PVDAQ polling
- load_historical_config(): Feature 002 — historical PVDAQ CSV ingestion
"""

from __future__ import annotations

import os
from dataclasses import dataclass


class ConfigurationError(Exception):
    """Raised when a required configuration setting is missing or invalid."""


@dataclass(frozen=True)
class Config:
    """Typed configuration for the PVDAQ ingestion function."""

    # OEDI Data Lake settings
    oedi_bucket_url: str
    oedi_systems_key: str
    oedi_data_prefix: str

    # Site selection (optional explicit list; if empty, discover from systems CSV)
    pvdaq_site_ids: list[int]
    pvdaq_site_count: int
    pvdaq_lookback_hours: int
    pvdaq_cron_schedule: str

    # Service Bus
    service_bus_queue_name: str
    dead_letter_queue_name: str
    service_bus_fully_qualified_namespace: str

    # Idempotency
    idempotency_table_name: str
    table_storage_uri: str

    # Metadata
    tenant_id: str
    mapping_version_pvdaq: str
    schema_version_pvdaq: str


@dataclass(frozen=True)
class HistoricalConfig:
    """Typed configuration for the PVDAQ historical ingestion function (feature 002)."""

    # OEDI Data Lake settings
    oedi_bucket_url: str
    oedi_historical_prefix: str

    # Historical site selection
    pvdaq_historical_site_ids: list[int]
    pvdaq_historical_cron_schedule: str

    # Service Bus
    pvdaq_historical_queue_name: str
    service_bus_queue_name: str
    dead_letter_queue_name: str
    service_bus_fully_qualified_namespace: str

    # File tracking
    file_tracking_table_name: str
    table_storage_uri: str

    # ADLS Gen2
    adls_account_url: str
    adls_container_name: str

    # Metadata
    tenant_id: str
    mapping_version_pvdaq: str
    schema_version_pvdaq: str


_REQUIRED_SETTINGS: list[str] = [
    "PVDAQ_LOOKBACK_HOURS",
    "PVDAQ_CRON_SCHEDULE",
    "SERVICE_BUS_QUEUE_NAME",
    "DEAD_LETTER_QUEUE_NAME",
    "IDEMPOTENCY_TABLE_NAME",
    "TableStorageConnection__tableServiceUri",
]

# Required only when ServiceBusConnection (connection string) is not set
_REQUIRED_WHEN_NO_CONN_STR: list[str] = [
    "ServiceBusConnection__fullyQualifiedNamespace",
]


def _parse_site_ids(raw: str) -> list[int]:
    """Parse a comma-separated string of site IDs into a list of integers."""
    try:
        return [int(s.strip()) for s in raw.split(",") if s.strip()]
    except ValueError as exc:
        raise ConfigurationError(
            f"PVDAQ_SITE_IDS must be a comma-separated list of integers, got: {raw!r}"
        ) from exc


def _require(name: str) -> str:
    """Return the value of an environment variable or raise ConfigurationError."""
    value = os.environ.get(name)
    if value is None or value.strip() == "":
        raise ConfigurationError(f"Missing required configuration setting: {name}")
    return value.strip()


def load_config() -> Config:
    """Load and validate configuration from environment variables.

    Returns:
        A fully populated Config instance.

    Raises:
        ConfigurationError: If any required setting is missing or invalid.
    """
    missing = [name for name in _REQUIRED_SETTINGS if not os.environ.get(name, "").strip()]
    if not os.environ.get("ServiceBusConnection", "").strip():
        missing += [n for n in _REQUIRED_WHEN_NO_CONN_STR if not os.environ.get(n, "").strip()]
    if missing:
        raise ConfigurationError(
            f"Missing required configuration setting(s): {', '.join(missing)}"
        )

    # PVDAQ_SITE_IDS is optional — if not set, sites will be discovered from OEDI systems CSV
    raw_site_ids = os.environ.get("PVDAQ_SITE_IDS", "").strip()
    site_ids = _parse_site_ids(raw_site_ids) if raw_site_ids else []

    return Config(
        oedi_bucket_url=os.environ.get(
            "OEDI_BUCKET_URL", "https://oedi-data-lake.s3.amazonaws.com"
        ).strip(),
        oedi_systems_key=os.environ.get(
            "OEDI_SYSTEMS_KEY", "pvdaq/csv/systems_20250729.csv"
        ).strip(),
        oedi_data_prefix=os.environ.get(
            "OEDI_DATA_PREFIX", "pvdaq/csv/pvdata"
        ).strip(),
        pvdaq_site_ids=site_ids,
        pvdaq_site_count=int(os.environ.get("PVDAQ_SITE_COUNT", "30").strip()),
        pvdaq_lookback_hours=int(_require("PVDAQ_LOOKBACK_HOURS")),
        pvdaq_cron_schedule=_require("PVDAQ_CRON_SCHEDULE"),
        service_bus_queue_name=_require("SERVICE_BUS_QUEUE_NAME"),
        dead_letter_queue_name=_require("DEAD_LETTER_QUEUE_NAME"),
        service_bus_fully_qualified_namespace=_require(
            "ServiceBusConnection__fullyQualifiedNamespace"
        ),
        idempotency_table_name=_require("IDEMPOTENCY_TABLE_NAME"),
        table_storage_uri=_require("TableStorageConnection__tableServiceUri"),
        tenant_id=os.environ.get("TENANT_ID", "research").strip(),
        mapping_version_pvdaq=os.environ.get("MAPPING_VERSION_PVDAQ", "unknown").strip(),
        schema_version_pvdaq=os.environ.get("SCHEMA_VERSION_PVDAQ", "v1").strip(),
    )


_HISTORICAL_REQUIRED_SETTINGS: list[str] = [
    "PVDAQ_HISTORICAL_SITE_IDS",
    "PVDAQ_HISTORICAL_CRON_SCHEDULE",
    "PVDAQ_HISTORICAL_QUEUE_NAME",
    "SERVICE_BUS_QUEUE_NAME",
    "DEAD_LETTER_QUEUE_NAME",
    "FILE_TRACKING_TABLE_NAME",
    "TableStorageConnection__tableServiceUri",
    "ADLS_ACCOUNT_URL",
    "ADLS_CONTAINER_NAME",
]


def load_historical_config() -> HistoricalConfig:
    """Load and validate configuration for the historical ingestion function.

    Returns:
        A fully populated HistoricalConfig instance.

    Raises:
        ConfigurationError: If any required setting is missing or invalid.
    """
    missing = [
        name for name in _HISTORICAL_REQUIRED_SETTINGS
        if not os.environ.get(name, "").strip()
    ]
    if not os.environ.get("ServiceBusConnection", "").strip():
        missing += [n for n in _REQUIRED_WHEN_NO_CONN_STR if not os.environ.get(n, "").strip()]
    if missing:
        raise ConfigurationError(
            f"Missing required configuration setting(s): {', '.join(missing)}"
        )

    raw_site_ids = _require("PVDAQ_HISTORICAL_SITE_IDS")
    site_ids = _parse_site_ids(raw_site_ids)

    return HistoricalConfig(
        oedi_bucket_url=os.environ.get(
            "OEDI_BUCKET_URL", "https://oedi-data-lake.s3.amazonaws.com"
        ).strip(),
        oedi_historical_prefix=os.environ.get(
            "OEDI_HISTORICAL_PREFIX", "pvdaq/2023-solar-data-prize"
        ).strip(),
        pvdaq_historical_site_ids=site_ids,
        pvdaq_historical_cron_schedule=_require("PVDAQ_HISTORICAL_CRON_SCHEDULE"),
        pvdaq_historical_queue_name=_require("PVDAQ_HISTORICAL_QUEUE_NAME"),
        service_bus_queue_name=_require("SERVICE_BUS_QUEUE_NAME"),
        dead_letter_queue_name=_require("DEAD_LETTER_QUEUE_NAME"),
        service_bus_fully_qualified_namespace=_require(
            "ServiceBusConnection__fullyQualifiedNamespace"
        ),
        file_tracking_table_name=_require("FILE_TRACKING_TABLE_NAME"),
        table_storage_uri=_require("TableStorageConnection__tableServiceUri"),
        adls_account_url=_require("ADLS_ACCOUNT_URL"),
        adls_container_name=_require("ADLS_CONTAINER_NAME"),
        tenant_id=os.environ.get("TENANT_ID", "default").strip(),
        mapping_version_pvdaq=os.environ.get("MAPPING_VERSION_PVDAQ", "unknown").strip(),
        schema_version_pvdaq=os.environ.get("SCHEMA_VERSION_PVDAQ", "v1").strip(),
    )
