"""Unit tests for src.adls_store — AdlsStore (ADR 0005: Blob API only).

Tests cover: _get_client dispatch (connection-string vs account-url +
DefaultAzureCredential), that stream_upload/write_json execute the identical
staged block-blob upload path regardless of which credential mode built the
client (each mode built via its own real SDK entry point, not an injected
client), that a source failure (404) touches no storage at all, and
close/context manager lifecycle.

Mocked: the Azure Storage Blob SDK and httpx — no real Azurite/Azure account
needed for these tests.
"""

from __future__ import annotations

import hashlib
import inspect
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from src import adls_store
from src.adls_store import AdlsBlobIdentityMismatchError, AdlsStore, AdlsUploadError


def _mock_blob_client() -> AsyncMock:
    blob_client = AsyncMock()
    blob_client.stage_block = AsyncMock(return_value=None)
    blob_client.commit_block_list = AsyncMock(return_value=None)
    blob_client.upload_blob = AsyncMock(return_value=None)
    # Default: blob doesn't exist yet — the identity-collision guard is a
    # no-op, matching a first-time upload to a fresh path.
    blob_client.get_blob_properties = AsyncMock(
        side_effect=ResourceNotFoundError("not found")
    )
    return blob_client


def _mock_container() -> AsyncMock:
    container = AsyncMock()
    container.create_container = AsyncMock(return_value=None)
    return container


def _mock_service_client(blob_client: AsyncMock | None = None) -> MagicMock:
    service_client = MagicMock()
    service_client.get_container_client = MagicMock(return_value=_mock_container())
    service_client.get_blob_client = MagicMock(
        return_value=blob_client or _mock_blob_client()
    )
    service_client.close = AsyncMock(return_value=None)
    return service_client


def _http_client_streaming(
    chunks: list[bytes],
    status_code: int = 200,
    raise_for_status_error: Exception | None = None,
) -> AsyncMock:
    """Build a mock httpx.AsyncClient whose .stream() yields the given chunks."""

    response = AsyncMock()
    response.status_code = status_code
    if raise_for_status_error is not None:
        response.raise_for_status = MagicMock(side_effect=raise_for_status_error)
    else:
        response.raise_for_status = MagicMock(return_value=None)

    async def _aiter_bytes(chunk_size: int = 0):
        for chunk in chunks:
            yield chunk

    response.aiter_bytes = _aiter_bytes

    class _StreamCtx:
        async def __aenter__(self):
            return response

        async def __aexit__(self, *exc_info):
            return False

    http_client = MagicMock()
    http_client.stream = MagicMock(return_value=_StreamCtx())
    http_client.aclose = AsyncMock(return_value=None)
    return http_client


class TestNoDataLakeImport:
    """ADR 0005: the DataLake (DFS) SDK is gone from this module entirely."""

    def test_source_has_no_filedatalake_reference(self) -> None:
        source = inspect.getsource(adls_store)
        assert "filedatalake" not in source
        assert "DataLakeServiceClient" not in source


class TestGetClientDispatch:
    """_get_client picks connection-string or account-url+credential — never both."""

    @pytest.mark.asyncio
    async def test_connection_string_mode_uses_from_connection_string(self) -> None:
        mock_client = MagicMock()

        with patch.object(
            adls_store.BlobServiceClient,
            "from_connection_string",
            return_value=mock_client,
        ) as mock_from_conn:
            store = AdlsStore(
                account_url="https://unused.blob.core.windows.net",
                container_name="bronze",
                connection_string="UseDevelopmentStorage=true",
            )
            client = await store._get_client()

        mock_from_conn.assert_called_once_with("UseDevelopmentStorage=true")
        assert client is mock_client
        assert store._credential is None

    @pytest.mark.asyncio
    async def test_credential_mode_uses_default_azure_credential(self) -> None:
        mock_cred = MagicMock()
        mock_client = MagicMock()

        with (
            patch("src.adls_store.DefaultAzureCredential", return_value=mock_cred),
            patch("src.adls_store.BlobServiceClient", return_value=mock_client),
        ):
            store = AdlsStore(
                account_url="https://acct.blob.core.windows.net",
                container_name="bronze",
            )
            client = await store._get_client()

        assert client is mock_client
        assert store._credential is mock_cred


class TestBothCredentialModesRunTheSameUploadPath:
    """stream_upload/write_json contain no branch on connection_string.

    Proven without injecting a pre-built ``service_client``: each mode is
    driven through the real SDK entry point it would use in production
    (``BlobServiceClient.from_connection_string`` for local/emulator,
    ``BlobServiceClient(account_url=..., credential=DefaultAzureCredential())``
    for cloud) via patching, and both are asserted to build their own client
    and then run the identical staged block-blob upload sequence."""

    @pytest.mark.asyncio
    async def test_stream_upload_identical_for_both_modes(self) -> None:
        chunks = [b"a,b,c\n1,2,3\n", b"4,5,6\n"]
        expected_hash = hashlib.sha256(b"".join(chunks)).hexdigest()
        expected_newlines = b"".join(chunks).count(b"\n")
        results = {}

        # --- local/emulator mode: BlobServiceClient.from_connection_string ---
        blob_client_local = _mock_blob_client()
        service_client_local = _mock_service_client(blob_client_local)
        with patch.object(
            adls_store.BlobServiceClient,
            "from_connection_string",
            return_value=service_client_local,
        ) as mock_from_conn:
            store = AdlsStore(
                account_url="https://unused.blob.core.windows.net",
                container_name="bronze",
                connection_string="UseDevelopmentStorage=true",
            )
            results["local_connection_string"] = await store.stream_upload(
                source_url="https://example.com/file.csv",
                file_path="source=pvdaq/dataset=1_ac_power/file.csv",
                s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/file.csv",
                http_client=_http_client_streaming(chunks),
            )

        mock_from_conn.assert_called_once_with("UseDevelopmentStorage=true")
        assert blob_client_local.stage_block.call_count == len(chunks)
        blob_client_local.commit_block_list.assert_called_once()
        staged_ids_local = [
            call.args[0] for call in blob_client_local.stage_block.call_args_list
        ]
        assert blob_client_local.commit_block_list.call_args.args[0] == staged_ids_local

        # --- cloud mode: account_url + DefaultAzureCredential ---
        blob_client_cloud = _mock_blob_client()
        service_client_cloud = _mock_service_client(blob_client_cloud)
        mock_cred = MagicMock()
        with (
            patch("src.adls_store.DefaultAzureCredential", return_value=mock_cred),
            patch(
                "src.adls_store.BlobServiceClient", return_value=service_client_cloud
            ) as mock_ctor,
        ):
            store = AdlsStore(
                account_url="https://acct.blob.core.windows.net",
                container_name="bronze",
            )
            results["cloud_credential"] = await store.stream_upload(
                source_url="https://example.com/file.csv",
                file_path="source=pvdaq/dataset=1_ac_power/file.csv",
                s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/file.csv",
                http_client=_http_client_streaming(chunks),
            )

        mock_ctor.assert_called_once_with(
            account_url="https://acct.blob.core.windows.net",
            credential=mock_cred,
        )
        assert blob_client_cloud.stage_block.call_count == len(chunks)
        blob_client_cloud.commit_block_list.assert_called_once()
        staged_ids_cloud = [
            call.args[0] for call in blob_client_cloud.stage_block.call_args_list
        ]
        assert blob_client_cloud.commit_block_list.call_args.args[0] == staged_ids_cloud

        # Each mode built its own, distinct client...
        assert service_client_local is not service_client_cloud
        # ...but ran the exact same upload sequence (same block IDs, same result).
        assert staged_ids_local == staged_ids_cloud
        assert results["local_connection_string"] == results["cloud_credential"]
        bytes_written, file_hash, newline_count = results["local_connection_string"]
        assert bytes_written == len(b"".join(chunks))
        assert file_hash == expected_hash
        assert newline_count == expected_newlines

    @pytest.mark.asyncio
    async def test_write_json_identical_for_both_modes(self) -> None:
        payload = {"dataset": {"dataset_id": "1_ac_power"}}

        # local/emulator mode
        blob_client_local = _mock_blob_client()
        service_client_local = _mock_service_client(blob_client_local)
        with patch.object(
            adls_store.BlobServiceClient,
            "from_connection_string",
            return_value=service_client_local,
        ):
            store = AdlsStore(
                account_url="https://unused.blob.core.windows.net",
                container_name="bronze",
                connection_string="UseDevelopmentStorage=true",
            )
            await store.write_json(
                "source=pvdaq/dataset=1_ac_power/metadata.json", payload
            )
        blob_client_local.upload_blob.assert_called_once()
        assert blob_client_local.upload_blob.call_args.kwargs.get("overwrite") is True

        # cloud/credential mode
        blob_client_cloud = _mock_blob_client()
        service_client_cloud = _mock_service_client(blob_client_cloud)
        with (
            patch("src.adls_store.DefaultAzureCredential", return_value=MagicMock()),
            patch(
                "src.adls_store.BlobServiceClient", return_value=service_client_cloud
            ),
        ):
            store = AdlsStore(
                account_url="https://acct.blob.core.windows.net",
                container_name="bronze",
            )
            await store.write_json(
                "source=pvdaq/dataset=1_ac_power/metadata.json", payload
            )
        blob_client_cloud.upload_blob.assert_called_once()
        assert blob_client_cloud.upload_blob.call_args.kwargs.get("overwrite") is True


class TestBlobIdentityCollisionGuard:
    """R2.1d item 1: the target blob's existing metadata is checked against
    the uploading s3_key before any block is staged, and every successful
    upload records its own s3_key in metadata for future uploads to check
    against.

    Motivating scenario: two distinct S3 objects (e.g. a dataset split
    across two files) can extract to the same site_id+category and
    therefore the same _adls_path+version — without this guard the second
    upload would silently overwrite the first file's data.
    """

    @pytest.mark.asyncio
    async def test_commit_stores_s3_key_in_blob_metadata(self) -> None:
        chunks = [b"a,b,c\n1,2,3\n"]
        blob_client = _mock_blob_client()
        service_client = _mock_service_client(blob_client)
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        await store.stream_upload(
            source_url="https://example.com/file.csv",
            file_path="source=pvdaq/dataset=1_ac_power/file_v1.csv",
            s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power_part1.csv",
            http_client=_http_client_streaming(chunks),
        )

        blob_client.commit_block_list.assert_called_once()
        assert blob_client.commit_block_list.call_args.kwargs.get("metadata") == {
            "s3_key": "pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power_part1.csv"
        }

    @pytest.mark.asyncio
    async def test_second_split_file_to_same_path_fails_loudly_no_overwrite(
        self,
    ) -> None:
        """The review's exact scenario: two split files that extract to the
        same category/path collide — the second must fail deterministically,
        never overwrite, and never stage a single block."""
        key_a = "pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power_part1.csv"
        key_b = "pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power_part2.csv"
        same_path = "source=pvdaq/dataset=1_ac_power/file_v1.csv"

        blob_client = _mock_blob_client()
        existing_props = MagicMock()
        existing_props.metadata = {"s3_key": key_a}
        blob_client.get_blob_properties = AsyncMock(return_value=existing_props)
        service_client = _mock_service_client(blob_client)
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        with pytest.raises(
            AdlsBlobIdentityMismatchError, match="Refusing to overwrite"
        ):
            await store.stream_upload(
                source_url="https://example.com/part2.csv",
                file_path=same_path,
                s3_key=key_b,
                http_client=_http_client_streaming([b"x,y\n1,2\n"]),
            )

        blob_client.stage_block.assert_not_called()
        blob_client.commit_block_list.assert_not_called()

    @pytest.mark.asyncio
    async def test_identity_mismatch_is_an_adls_upload_error_subclass(self) -> None:
        """Deterministic classification (function_app._is_transient) relies
        on this being an AdlsUploadError — no special-casing needed."""
        assert issubclass(AdlsBlobIdentityMismatchError, AdlsUploadError)

    @pytest.mark.asyncio
    async def test_reupload_with_same_s3_key_is_not_a_collision(self) -> None:
        """A retry/redelivery re-uploading the identical file is fine — the
        guard only rejects a *different* s3_key at the same path."""
        key_a = "pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power_part1.csv"

        blob_client = _mock_blob_client()
        existing_props = MagicMock()
        existing_props.metadata = {"s3_key": key_a}
        blob_client.get_blob_properties = AsyncMock(return_value=existing_props)
        service_client = _mock_service_client(blob_client)
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        await store.stream_upload(
            source_url="https://example.com/part1.csv",
            file_path="source=pvdaq/dataset=1_ac_power/file_v1.csv",
            s3_key=key_a,
            http_client=_http_client_streaming([b"x,y\n1,2\n"]),
        )

        blob_client.commit_block_list.assert_called_once()

    @pytest.mark.asyncio
    async def test_blob_with_no_recorded_s3_key_is_not_a_collision(self) -> None:
        """A pre-existing blob written before this guard existed has no
        s3_key metadata — nothing to compare against, so it's not treated
        as a collision."""
        blob_client = _mock_blob_client()
        existing_props = MagicMock()
        existing_props.metadata = {}
        blob_client.get_blob_properties = AsyncMock(return_value=existing_props)
        service_client = _mock_service_client(blob_client)
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        await store.stream_upload(
            source_url="https://example.com/file.csv",
            file_path="source=pvdaq/dataset=1_ac_power/file_v1.csv",
            s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/9068_ac_power.csv",
            http_client=_http_client_streaming([b"x,y\n1,2\n"]),
        )

        blob_client.commit_block_list.assert_called_once()


class TestStreamUploadErrors:
    """A source-side failure must not touch storage at all — the GET is
    attempted before the container or blob client are ever created, so a 404
    leaves no blob (not even an empty one) behind."""

    @pytest.mark.asyncio
    async def test_404_touches_no_storage_and_raises(self) -> None:
        service_client = _mock_service_client()
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )
        http_client = _http_client_streaming([], status_code=404)

        with pytest.raises(AdlsUploadError, match="Source not found"):
            await store.stream_upload(
                source_url="https://example.com/missing.csv",
                file_path="file.csv",
                s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/missing.csv",
                http_client=http_client,
            )

        service_client.get_container_client.assert_not_called()
        service_client.get_blob_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_404_error_status_touches_no_storage(self) -> None:
        import httpx

        service_client = _mock_service_client()
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )
        error = httpx.HTTPStatusError(
            "Server error", request=MagicMock(), response=MagicMock(status_code=500)
        )
        http_client = _http_client_streaming(
            [], status_code=500, raise_for_status_error=error
        )

        with pytest.raises(AdlsUploadError):
            await store.stream_upload(
                source_url="https://example.com/broken.csv",
                file_path="file.csv",
                s3_key="pvdaq/2023-solar-data-prize/1_OEDI/data/broken.csv",
                http_client=http_client,
            )

        service_client.get_container_client.assert_not_called()
        service_client.get_blob_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_container_already_exists_is_ignored(self) -> None:
        blob_client = _mock_blob_client()
        service_client = MagicMock()
        container = AsyncMock()
        container.create_container = AsyncMock(
            side_effect=ResourceExistsError("already exists")
        )
        service_client.get_container_client = MagicMock(return_value=container)
        service_client.get_blob_client = MagicMock(return_value=blob_client)
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        await store.write_json("metadata.json", {"a": 1})

        blob_client.upload_blob.assert_called_once()


class TestLifecycle:
    """Async context manager and close() ownership semantics."""

    @pytest.mark.asyncio
    async def test_injected_client_not_closed(self) -> None:
        service_client = _mock_service_client()
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )

        await store.close()

        service_client.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_owned_client_and_credential_closed(self) -> None:
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
        )
        mock_client = AsyncMock()
        mock_cred = AsyncMock()
        store._service_client = mock_client
        store._credential = mock_cred

        await store.close()

        mock_client.close.assert_called_once()
        mock_cred.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        service_client = _mock_service_client()
        store = AdlsStore(
            account_url="https://acct.blob.core.windows.net",
            container_name="bronze",
            service_client=service_client,
        )
        async with store as s:
            assert s is store
