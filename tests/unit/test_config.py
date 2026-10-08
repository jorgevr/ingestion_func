"""Unit tests for src.config — configuration loader."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from src.config import (
    ConfigurationError,
    HistoricalConfig,
    _require,
    _validate_mapping_version,
    load_historical_config,
)


class TestValidateMappingVersionDirectly:
    """Direct tests for the _validate_mapping_version helper (R2.3b F3):
    MAPPING_VERSION_PVDAQ must match ^(v[0-9]+|unknown)$, mirroring
    contracts/dataset-available.v1.json's mapping_version pattern."""

    @pytest.mark.parametrize("value", ["unknown", "v1", "v2", "v23"])
    def test_accepts_valid_values(self, value: str) -> None:
        assert _validate_mapping_version(value) == value

    @pytest.mark.parametrize(
        "value", ["", "1.2.3", "V1", "version1", "v", "vNaN", " unknown"]
    )
    def test_rejects_invalid_values(self, value: str) -> None:
        with pytest.raises(ConfigurationError, match="MAPPING_VERSION_PVDAQ"):
            _validate_mapping_version(value)


class TestMappingVersionValidatedAtStartup:
    """R2.3b F3 acceptance: a bad MAPPING_VERSION_PVDAQ fails host start
    (ConfigurationError from the loader itself, naming the variable) — not
    a per-message jsonschema failure discovered later inside
    build_dataset_envelope."""

    def test_load_historical_config_cloud_mode_only_with_bad_mapping_version_raises(
        self,
    ) -> None:
        """load_historical_config, cloud-only settings (namespace +
        account URL; no connection strings at all)."""
        env = _historical_env()
        env["MAPPING_VERSION_PVDAQ"] = "V1"
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="MAPPING_VERSION_PVDAQ"),
        ):
            load_historical_config()


class TestRequireDirectly:
    """Direct tests for the _require helper function."""

    def test_require_raises_on_missing_var(self) -> None:
        """_require raises ConfigurationError when the env var is not set."""
        with (
            patch.dict(os.environ, {}, clear=True),
            pytest.raises(
                ConfigurationError,
                match="Missing required configuration setting: NONEXISTENT",
            ),
        ):
            _require("NONEXISTENT")

    def test_require_raises_on_empty_string(self) -> None:
        with (
            patch.dict(os.environ, {"EMPTY_VAR": ""}, clear=True),
            pytest.raises(ConfigurationError, match="EMPTY_VAR"),
        ):
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
        "SERVICE_BUS_QUEUE_NAME": "raw-energy-events",
        "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
        "ServiceBusConnection__fullyQualifiedNamespace": "test-sb.servicebus.windows.net",
        "FILE_TRACKING_TABLE_NAME": "PvdaqFileTracking",
        "TableStorageConnection__tableServiceUri": "https://teststorage.table.core.windows.net",
        "DATA_STORAGE_ACCOUNT_URL": "https://testaccount.blob.core.windows.net",
        "BRONZE_CONTAINER": "raw",
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
        assert cfg.service_bus_queue_name == "raw-energy-events"
        assert cfg.dead_letter_queue_name == "pvdaq-dead-letter"
        assert (
            cfg.data_storage_account_url == "https://testaccount.blob.core.windows.net"
        )
        assert cfg.bronze_container == "raw"

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
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="comma-separated list of integers"),
        ):
            load_historical_config()


class TestHistoricalMissingRequired:
    """Tests that missing required historical settings raise ConfigurationError."""

    def test_missing_site_ids_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_SITE_IDS"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_SITE_IDS"),
        ):
            load_historical_config()

    def test_missing_queue_name_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_QUEUE_NAME"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_QUEUE_NAME"),
        ):
            load_historical_config()

    def test_missing_file_tracking_table_raises(self) -> None:
        env = _historical_env()
        del env["FILE_TRACKING_TABLE_NAME"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="FILE_TRACKING_TABLE_NAME"),
        ):
            load_historical_config()

    def test_missing_cron_schedule_raises(self) -> None:
        env = _historical_env()
        del env["PVDAQ_HISTORICAL_CRON_SCHEDULE"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="PVDAQ_HISTORICAL_CRON_SCHEDULE"),
        ):
            load_historical_config()

    def test_missing_bronze_container_raises(self) -> None:
        env = _historical_env()
        del env["BRONZE_CONTAINER"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="BRONZE_CONTAINER"),
        ):
            load_historical_config()


class TestDataStorageConnectionOrAccountUrlRequired:
    """docs/contracts.md 'Shared configuration': at least one of
    DATA_STORAGE_CONNECTION / DATA_STORAGE_ACCOUNT_URL must be set, or
    config loading fails fast rather than deferring to a confusing SDK
    error deep inside AdlsStore."""

    def test_neither_set_raises(self) -> None:
        env = _historical_env()
        del env["DATA_STORAGE_ACCOUNT_URL"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError, match="DATA_STORAGE_ACCOUNT_URL"),
        ):
            load_historical_config()

    def test_connection_string_only_succeeds_with_blank_account_url(self) -> None:
        env = _historical_env()
        del env["DATA_STORAGE_ACCOUNT_URL"]
        env["DATA_STORAGE_CONNECTION"] = "UseDevelopmentStorage=true"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.data_storage_account_url == ""
        assert cfg.bronze_container == "raw"

    def test_account_url_only_succeeds(self) -> None:
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert (
            cfg.data_storage_account_url == "https://testaccount.blob.core.windows.net"
        )

    def test_both_set_prefers_neither_fails(self) -> None:
        """Both present is not an error — DATA_STORAGE_CONNECTION still wins
        at the AdlsStore level; config loading only asserts at-least-one."""
        env = _historical_env()
        env["DATA_STORAGE_CONNECTION"] = "UseDevelopmentStorage=true"
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert (
            cfg.data_storage_account_url == "https://testaccount.blob.core.windows.net"
        )


class TestHistoricalServiceBusModes:
    """load_historical_config: the local (connection string) vs cloud
    (namespace + managed identity) Service Bus split — regression coverage
    for src/config.py:234's unconditional _require bug."""

    def test_local_mode_connection_string_only_succeeds(self) -> None:
        """Only ServiceBusConnection set, no …__fullyQualifiedNamespace at all."""
        env = _historical_env()
        del env["ServiceBusConnection__fullyQualifiedNamespace"]
        env["ServiceBusConnection"] = (
            "Endpoint=sb://localhost;SharedAccessKeyName=Root;"
            "SharedAccessKey=test;UseDevelopmentEmulator=true;"
        )
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert cfg.service_bus_fully_qualified_namespace == ""

    def test_cloud_mode_namespace_only_succeeds(self) -> None:
        """Only …__fullyQualifiedNamespace set, no ServiceBusConnection at all."""
        env = _historical_env()
        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert (
            cfg.service_bus_fully_qualified_namespace
            == "test-sb.servicebus.windows.net"
        )

    def test_missing_both_raises(self) -> None:
        env = _historical_env()
        del env["ServiceBusConnection__fullyQualifiedNamespace"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(
                ConfigurationError,
                match="ServiceBusConnection__fullyQualifiedNamespace",
            ),
        ):
            load_historical_config()


class TestMissingBothModesNamesBothKeys:
    """When neither Service Bus nor storage alternative is configured, the
    single ConfigurationError must name both missing keys — not stop at the
    first, and not silently proceed with a half-built config."""

    def test_missing_all_mode_dependent_settings_names_both_keys(self) -> None:
        env = _historical_env()
        del env["ServiceBusConnection__fullyQualifiedNamespace"]
        del env["DATA_STORAGE_ACCOUNT_URL"]
        with (
            patch.dict(os.environ, env, clear=True),
            pytest.raises(ConfigurationError) as exc_info,
        ):
            load_historical_config()

        message = str(exc_info.value)
        assert "ServiceBusConnection__fullyQualifiedNamespace" in message
        assert "DATA_STORAGE_ACCOUNT_URL" in message


class TestLocalSettingsTemplateSatisfiesConfig:
    """R2.9b item 10: local.settings.json.template's keys (after renaming
    SERVICE_BUS_TOPIC_NAME -> SERVICE_BUS_QUEUE_NAME and dropping the
    feature-001-only keys) must satisfy load_historical_config() — no
    'Missing required configuration setting' — so a fresh clone that copies
    the template to local.settings.json and fills in placeholders has a
    config that actually loads."""

    def test_template_values_satisfy_load_historical_config(self) -> None:
        template_path = (
            Path(__file__).resolve().parent.parent.parent
            / "local.settings.json.template"
        )
        template = json.loads(template_path.read_text(encoding="utf-8"))
        env = {k: v for k, v in template["Values"].items()}

        with patch.dict(os.environ, env, clear=True):
            cfg = load_historical_config()

        assert isinstance(cfg, HistoricalConfig)
