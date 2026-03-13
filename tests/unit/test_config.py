"""Unit tests for src.config — configuration loader."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from src.config import (
    Config,
    ConfigurationError,
    HistoricalConfig,
    _require,
    load_config,
    load_historical_config,
)


def _base_env() -> dict[str, str]:
    """Return a minimal valid set of environment variables."""
    return {
        "PVDAQ_SITE_IDS": "2",
        "PVDAQ_LOOKBACK_HOURS": "24",
        "PVDAQ_CRON_SCHEDULE": "0 */15 * * * *",
        "SERVICE_BUS_TOPIC_NAME": "energy-telemetry-ingested",
        "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
        "ServiceBusConnection__fullyQualifiedNamespace": "test-sb.servicebus.windows.net",
        "IDEMPOTENCY_TABLE_NAME": "PvdaqIdempotency",
        "TableStorageConnection__tableServiceUri": "https://teststorage.table.core.windows.net",
    }


class TestLoadConfigValid:
    """Tests that valid environment variables produce a correctly typed Config."""

    def test_valid_env_returns_config(self) -> None:
        env = _base_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert isinstance(cfg, Config)
        assert cfg.pvdaq_site_ids == [2]
        assert cfg.pvdaq_lookback_hours == 24
        assert cfg.pvdaq_cron_schedule == "0 */15 * * * *"
        assert cfg.service_bus_topic_name == "energy-telemetry-ingested"
        assert cfg.dead_letter_queue_name == "pvdaq-dead-letter"
        assert cfg.service_bus_fully_qualified_namespace == "test-sb.servicebus.windows.net"
        assert cfg.idempotency_table_name == "PvdaqIdempotency"
        assert cfg.table_storage_uri == "https://teststorage.table.core.windows.net"

    def test_lookback_hours_parsed_as_int(self) -> None:
        env = _base_env()
        env["PVDAQ_LOOKBACK_HOURS"] = "48"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_lookback_hours == 48
        assert isinstance(cfg.pvdaq_lookback_hours, int)


class TestSiteIdsParsing:
    """Tests for PVDAQ_SITE_IDS comma-separated parsing."""

    def test_single_site_id(self) -> None:
        env = _base_env()
        env["PVDAQ_SITE_IDS"] = "2"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_site_ids == [2]

    def test_multiple_site_ids(self) -> None:
        env = _base_env()
        env["PVDAQ_SITE_IDS"] = "2,34,56"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_site_ids == [2, 34, 56]

    def test_site_ids_with_spaces(self) -> None:
        env = _base_env()
        env["PVDAQ_SITE_IDS"] = "2, 34"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_site_ids == [2, 34]

    def test_empty_site_ids_returns_empty_list(self) -> None:
        env = _base_env()
        del env["PVDAQ_SITE_IDS"]
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_site_ids == []


class TestOediDefaults:
    """Tests that OEDI settings fall back to their defaults."""

    def test_oedi_bucket_url_default(self) -> None:
        env = _base_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.oedi_bucket_url == "https://oedi-data-lake.s3.amazonaws.com"

    def test_oedi_systems_key_default(self) -> None:
        env = _base_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.oedi_systems_key == "pvdaq/csv/systems_20250729.csv"

    def test_oedi_data_prefix_default(self) -> None:
        env = _base_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.oedi_data_prefix == "pvdaq/csv/pvdata"

    def test_site_count_default(self) -> None:
        env = _base_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.pvdaq_site_count == 30

    def test_custom_oedi_settings(self) -> None:
        env = _base_env()
        env["OEDI_BUCKET_URL"] = "https://custom-bucket.s3.amazonaws.com"
        env["OEDI_SYSTEMS_KEY"] = "pvdaq/csv/systems_20260101.csv"
        env["PVDAQ_SITE_COUNT"] = "10"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.oedi_bucket_url == "https://custom-bucket.s3.amazonaws.com"
        assert cfg.oedi_systems_key == "pvdaq/csv/systems_20260101.csv"
        assert cfg.pvdaq_site_count == 10


class TestDefaults:
    """Tests that optional settings fall back to their documented defaults."""

    def test_tenant_id_defaults_to_research(self) -> None:
        env = _base_env()
        env.pop("TENANT_ID", None)
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.tenant_id == "research"

    def test_mapping_version_defaults_to_unknown(self) -> None:
        env = _base_env()
        env.pop("MAPPING_VERSION_PVDAQ", None)
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.mapping_version_pvdaq == "unknown"

    def test_schema_version_defaults_to_v1(self) -> None:
        env = _base_env()
        env.pop("SCHEMA_VERSION_PVDAQ", None)
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.schema_version_pvdaq == "v1"

    def test_explicit_tenant_id_overrides_default(self) -> None:
        env = _base_env()
        env["TENANT_ID"] = "production"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.tenant_id == "production"

    def test_explicit_mapping_version_overrides_default(self) -> None:
        env = _base_env()
        env["MAPPING_VERSION_PVDAQ"] = "1.2.3"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_config()

        assert cfg.mapping_version_pvdaq == "1.2.3"


class TestMissingRequired:
    """Tests that missing required settings raise ConfigurationError."""

    def test_missing_service_bus_topic_raises(self) -> None:
        env = _base_env()
        del env["SERVICE_BUS_TOPIC_NAME"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="SERVICE_BUS_TOPIC_NAME"):
                load_config()

    def test_missing_lookback_hours_raises(self) -> None:
        env = _base_env()
        del env["PVDAQ_LOOKBACK_HOURS"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="PVDAQ_LOOKBACK_HOURS"):
                load_config()

    def test_missing_multiple_settings_lists_all(self) -> None:
        env = _base_env()
        del env["SERVICE_BUS_TOPIC_NAME"]
        del env["DEAD_LETTER_QUEUE_NAME"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="SERVICE_BUS_TOPIC_NAME"):
                load_config()

    def test_empty_string_treated_as_missing(self) -> None:
        env = _base_env()
        env["SERVICE_BUS_TOPIC_NAME"] = ""
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="SERVICE_BUS_TOPIC_NAME"):
                load_config()

    def test_whitespace_only_treated_as_missing(self) -> None:
        env = _base_env()
        env["SERVICE_BUS_TOPIC_NAME"] = "   "
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="SERVICE_BUS_TOPIC_NAME"):
                load_config()

    def test_invalid_site_ids_raises(self) -> None:
        """Non-integer values in PVDAQ_SITE_IDS raise ConfigurationError."""
        env = _base_env()
        env["PVDAQ_SITE_IDS"] = "2,abc,34"
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="comma-separated list of integers"):
                load_config()


class TestRequireDirectly:
    """Direct tests for the _require helper function."""

    def test_require_raises_on_missing_var(self) -> None:
        """_require raises ConfigurationError when the env var is not set."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigurationError, match="Missing required configuration setting: NONEXISTENT"):
                _require("NONEXISTENT")

    def test_require_raises_on_empty_string(self) -> None:
        with patch.dict(os.environ, {"EMPTY_VAR": ""}, clear=True):
            with pytest.raises(ConfigurationError, match="EMPTY_VAR"):
                _require("EMPTY_VAR")

    def test_require_returns_stripped_value(self) -> None:
        with patch.dict(os.environ, {"MY_VAR": "  hello  "}, clear=True):
            assert _require("MY_VAR") == "hello"


# ---------------------------------------------------------------------------
# Feature 002: HistoricalConfig tests
# ---------------------------------------------------------------------------


def _historical_env() -> dict[str, str]:
    """Return a minimal valid set of environment variables for historical config."""
    return {
        "PVDAQ_HISTORICAL_SITE_IDS": "9068,9069,2107,7333",
        "PVDAQ_HISTORICAL_CRON_SCHEDULE": "0 0 */6 * * *",
        "PVDAQ_HISTORICAL_QUEUE_NAME": "pvdaq-historical-work",
        "SERVICE_BUS_TOPIC_NAME": "raw-energy-events",
        "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
        "ServiceBusConnection__fullyQualifiedNamespace": "test-sb.servicebus.windows.net",
        "FILE_TRACKING_TABLE_NAME": "PvdaqFileTracking",
        "TableStorageConnection__tableServiceUri": "https://teststorage.table.core.windows.net",
        "ADLS_ACCOUNT_URL": "https://testaccount.dfs.core.windows.net",
        "ADLS_CONTAINER_NAME": "raw",
    }


class TestLoadHistoricalConfigValid:
    """Tests that valid env vars produce a correctly typed HistoricalConfig."""

    def test_valid_env_returns_historical_config(self) -> None:
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert isinstance(cfg, HistoricalConfig)
        assert cfg.pvdaq_historical_site_ids == [9068, 9069, 2107, 7333]
        assert cfg.pvdaq_historical_cron_schedule == "0 0 */6 * * *"
        assert cfg.pvdaq_historical_queue_name == "pvdaq-historical-work"
        assert cfg.file_tracking_table_name == "PvdaqFileTracking"
        assert cfg.service_bus_topic_name == "raw-energy-events"
        assert cfg.dead_letter_queue_name == "pvdaq-dead-letter"
        assert cfg.adls_account_url == "https://testaccount.dfs.core.windows.net"
        assert cfg.adls_container_name == "raw"

    def test_oedi_historical_prefix_default(self) -> None:
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.oedi_historical_prefix == "pvdaq/2023-solar-data-prize"

    def test_custom_oedi_historical_prefix(self) -> None:
        env = _historical_env()
        env["OEDI_HISTORICAL_PREFIX"] = "pvdaq/custom-path"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.oedi_historical_prefix == "pvdaq/custom-path"

    def test_metadata_defaults(self) -> None:
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.tenant_id == "default"
        assert cfg.mapping_version_pvdaq == "unknown"
        assert cfg.schema_version_pvdaq == "v1"


class TestHistoricalSiteIdsParsing:
    """Tests for PVDAQ_HISTORICAL_SITE_IDS parsing."""

    def test_four_site_ids(self) -> None:
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.pvdaq_historical_site_ids == [9068, 9069, 2107, 7333]

    def test_single_site_id(self) -> None:
        env = _historical_env()
        env["PVDAQ_HISTORICAL_SITE_IDS"] = "9068"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.pvdaq_historical_site_ids == [9068]

    def test_site_ids_with_spaces(self) -> None:
        env = _historical_env()
        env["PVDAQ_HISTORICAL_SITE_IDS"] = "9068, 9069, 2107"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.pvdaq_historical_site_ids == [9068, 9069, 2107]

    def test_invalid_site_ids_raises(self) -> None:
        env = _historical_env()
        env["PVDAQ_HISTORICAL_SITE_IDS"] = "9068,bad,2107"
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="comma-separated list of integers"):
                load_historical_config()


class TestHistoricalMissingRequired:
    """Tests that missing required historical settings raise ConfigurationError."""

    def test_missing_site_ids_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_SITE_IDS"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_SITE_IDS"):
                load_historical_config()

    def test_missing_queue_name_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_QUEUE_NAME"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_QUEUE_NAME"):
                load_historical_config()

    def test_missing_file_tracking_table_raises(self) -> None:
        env = _historical_env()
        del env["FILE_TRACKING_TABLE_NAME"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="FILE_TRACKING_TABLE_NAME"):
                load_historical_config()

    def test_missing_cron_schedule_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_CRON_SCHEDULE"]
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_CRON_SCHEDULE"):
                load_historical_config()
