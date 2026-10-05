"""Unit tests for function_app's storage config switch and AdlsStore.blob_url
(ADR 0005: Blob API only; shared config names, docs/contracts.md).

Covers the collapse of the ``STORAGE_EMULATOR`` boolean flag into a single
connection-string-or-credential switch (``_storage_connection_string``, now
keyed on ``DATA_STORAGE_CONNECTION``), and that ``AdlsStore.blob_url`` — the
SDK-built replacement for the old hand-rolled ``_adls_uri`` — keeps
``storage_path`` valid against the registry contract's ``data.storage_path``
rule even when a path segment contains a space.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import jsonschema
import pytest

from function_app import _storage_connection_string
from src.adls_store import AdlsStore

# A local copy of contracts/dataset-available.v1.json's data.storage_path
# sub-schema (root workspace repo, Contract Owner-only), not the vendored
# copy: services/ingestion-func has no schemas/contracts/ directory yet
# (R2.3's vendoring step — contracts/vendoring.json's "ingestion-func" entry
# is still empty) — reading the real file would make this test depend on
# the workspace layout, which fails in this repo's own standalone clone
# (CI-1). Copied here instead of looked up so this test runs identically in
# both. Once R2.3 lands the vendored copy, prefer loading it from
# schemas/contracts/ over this literal; drift between this literal and the
# registry is caught only by the workspace root's scripts/check-contracts.py
# (R1.2), not by this service's own CI.
_STORAGE_PATH_SCHEMA = {
    "type": "string",
    "pattern": "^https?://[^ ]+$",
}

_FAKE_LOCAL_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;"
)


class TestStorageConnectionString:
    """_storage_connection_string reads DATA_STORAGE_CONNECTION directly —
    no STORAGE_EMULATOR boolean flag and no hardcoded sentinel value."""

    def test_returns_none_when_unset(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            assert _storage_connection_string() is None

    def test_returns_configured_connection_string(self) -> None:
        with patch.dict(
            os.environ, {"DATA_STORAGE_CONNECTION": _FAKE_LOCAL_CONNECTION_STRING}
        ):
            assert _storage_connection_string() == _FAKE_LOCAL_CONNECTION_STRING

    def test_blank_value_treated_as_unset(self) -> None:
        with patch.dict(os.environ, {"DATA_STORAGE_CONNECTION": "   "}):
            assert _storage_connection_string() is None

    def test_storage_emulator_flag_is_no_longer_consulted(self) -> None:
        """The old STORAGE_EMULATOR=true flag has no effect post-collapse."""
        with patch.dict(os.environ, {"STORAGE_EMULATOR": "true"}, clear=True):
            assert _storage_connection_string() is None

    def test_old_azure_storage_connection_name_is_no_longer_consulted(self) -> None:
        """The pre-rename AZURE_STORAGE_CONNECTION name has no effect."""
        with patch.dict(
            os.environ,
            {"AZURE_STORAGE_CONNECTION": _FAKE_LOCAL_CONNECTION_STRING},
            clear=True,
        ):
            assert _storage_connection_string() is None

    def test_logs_connection_string_mode(self) -> None:
        logger = MagicMock()
        with patch.dict(os.environ, {"DATA_STORAGE_CONNECTION": "conn-str"}):
            _storage_connection_string(logger)

        logger.info.assert_called_once()
        args = logger.info.call_args.args
        assert "connection_string" in args

    def test_logs_credential_mode(self) -> None:
        logger = MagicMock()
        with patch.dict(os.environ, {}, clear=True):
            _storage_connection_string(logger)

        logger.info.assert_called_once()
        args = logger.info.call_args.args
        assert "default_azure_credential" in args

    def test_no_logger_means_no_logging_attempted(self) -> None:
        # Should not raise even though no logger is passed (default None).
        with patch.dict(os.environ, {}, clear=True):
            assert _storage_connection_string() is None


class TestBlobUrl:
    """AdlsStore.blob_url builds storage_path from the real SDK's
    BlobClient.url — not hand-assembled — for both credential modes.
    Building the client and reading .url makes no network call, so these
    run fully offline against the real azure-storage-blob SDK."""

    @pytest.mark.asyncio
    async def test_local_connection_string_mode(self) -> None:
        async with AdlsStore(
            account_url="https://unused.blob.core.windows.net",
            container_name="bronze",
            connection_string=_FAKE_LOCAL_CONNECTION_STRING,
        ) as store:
            url = await store.blob_url("a/b.csv")

        assert url == "http://azurite:10000/devstoreaccount1/bronze/a/b.csv"

    @pytest.mark.asyncio
    async def test_cloud_credential_mode(self) -> None:
        with patch("src.adls_store.DefaultAzureCredential", return_value=AsyncMock()):
            async with AdlsStore(
                account_url="https://acct.blob.core.windows.net",
                container_name="bronze",
            ) as store:
                url = await store.blob_url("a/b.csv")

        assert url == "https://acct.blob.core.windows.net/bronze/a/b.csv"
        assert "abfss://" not in url

    @pytest.mark.asyncio
    async def test_encodes_space_in_path(self) -> None:
        async with AdlsStore(
            account_url="https://unused.blob.core.windows.net",
            container_name="bronze",
            connection_string=_FAKE_LOCAL_CONNECTION_STRING,
        ) as store:
            url = await store.blob_url("source=pvdaq/dataset=1_ac power/file.csv")

        assert " " not in url
        assert "ac%20power" in url

    @pytest.mark.asyncio
    async def test_both_modes_agree_on_the_same_path(self) -> None:
        """The SDK-built URL differs only in host/scheme between modes —
        same container, same (encoded) blob path either way."""
        file_path = "source=pvdaq/dataset=9068_ac power/file.csv"

        async with AdlsStore(
            account_url="https://unused.blob.core.windows.net",
            container_name="bronze",
            connection_string=_FAKE_LOCAL_CONNECTION_STRING,
        ) as local_store:
            local_url = await local_store.blob_url(file_path)

        with patch("src.adls_store.DefaultAzureCredential", return_value=AsyncMock()):
            async with AdlsStore(
                account_url="https://acct.blob.core.windows.net",
                container_name="bronze",
            ) as cloud_store:
                cloud_url = await cloud_store.blob_url(file_path)

        local_suffix = local_url.split("/devstoreaccount1/", 1)[1]
        cloud_suffix = cloud_url.split("://acct.blob.core.windows.net/", 1)[1]
        assert local_suffix == cloud_suffix


class TestStoragePathContractValidation:
    """A storage_path built from a filename containing a space must still
    validate against the registry contract's pattern (``^https?://[^ ]+$``,
    ``data.storage_path`` in ``contracts/dataset-available.v1.json`` at the
    workspace root — copied inline above as ``_STORAGE_PATH_SCHEMA``, see
    that comment) — now produced by AdlsStore.blob_url (SDK-built), not a
    hand-rolled helper."""

    @pytest.mark.asyncio
    async def test_encoded_space_validates_against_contract(self) -> None:
        validator = jsonschema.Draft202012Validator(_STORAGE_PATH_SCHEMA)

        async with AdlsStore(
            account_url="https://unused.blob.core.windows.net",
            container_name="bronze",
            connection_string=_FAKE_LOCAL_CONNECTION_STRING,
        ) as store:
            url = await store.blob_url(
                "source=pvdaq/dataset=9068_ac power/"
                "ingestion_date=2026-01-01/9068_ac power_v1.csv"
            )

        errors = list(validator.iter_errors(url))
        assert errors == [], f"storage_path failed contract validation: {errors}"

    def test_literal_space_would_fail_contract(self) -> None:
        """Sanity check on the schema itself: an unencoded space is rejected,
        which is exactly the defect this fix closes."""
        validator = jsonschema.Draft202012Validator(_STORAGE_PATH_SCHEMA)

        unencoded = "https://acct.blob.core.windows.net/bronze/source=pvdaq/dataset=9068_ac power/file.csv"

        with pytest.raises(jsonschema.ValidationError):
            validator.validate(unencoded)
