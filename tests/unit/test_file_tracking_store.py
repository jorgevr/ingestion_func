"""Unit tests for src.file_tracking_store — FileTrackingStore."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from src.file_tracking_store import FileTrackingStore


def _make_store(table_client: AsyncMock | None = None) -> FileTrackingStore:
    """Create a FileTrackingStore with a mock TableClient."""
    return FileTrackingStore(
        table_name="PvdaqFileTracking",
        table_service_uri="https://test.table.core.windows.net",
        table_client=table_client,
    )


SITE_ID = 9068
S3_KEY = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_data.csv"
FILE_INFO = {"key": S3_KEY, "size": 65000000, "last_modified": "2024-01-15T12:00:00Z"}


class TestRowKey:
    """RowKey is a deterministic SHA-256 hash truncated to 64 chars."""

    def test_row_key_is_64_chars(self) -> None:
        rk = FileTrackingStore._row_key(S3_KEY)
        assert len(rk) == 64

    def test_row_key_deterministic(self) -> None:
        assert FileTrackingStore._row_key(S3_KEY) == FileTrackingStore._row_key(S3_KEY)

    def test_different_keys_produce_different_hashes(self) -> None:
        rk1 = FileTrackingStore._row_key("a.csv")
        rk2 = FileTrackingStore._row_key("b.csv")
        assert rk1 != rk2


class TestPartitionKey:
    """PartitionKey is the site ID as a string."""

    def test_partition_key(self) -> None:
        assert FileTrackingStore._partition_key(9068) == "9068"


class TestGetUnprocessedFiles:
    """get_unprocessed_files compares S3 listing against tracked entities."""

    @pytest.mark.asyncio
    async def test_new_file_returned(self) -> None:
        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(
            side_effect=ResourceNotFoundError("Not found"),
        )
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [FILE_INFO])
        assert result == [FILE_INFO]

    @pytest.mark.asyncio
    async def test_completed_same_size_filtered_out(self) -> None:
        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(return_value={
            "Status": "completed",
            "Size": 65000000,
            "LastModified": "2024-01-15T12:00:00Z",
        })
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [FILE_INFO])
        assert result == []

    @pytest.mark.asyncio
    async def test_changed_size_returned(self) -> None:
        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(return_value={
            "Status": "completed",
            "Size": 50000000,  # different size
            "LastModified": "2024-01-15T12:00:00Z",
        })
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [FILE_INFO])
        assert result == [FILE_INFO]

    @pytest.mark.asyncio
    async def test_changed_last_modified_returned(self) -> None:
        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(return_value={
            "Status": "completed",
            "Size": 65000000,
            "LastModified": "2024-02-01T00:00:00Z",  # different date
        })
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [FILE_INFO])
        assert result == [FILE_INFO]

    @pytest.mark.asyncio
    async def test_failed_status_returned(self) -> None:
        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(return_value={
            "Status": "failed",
            "Size": 65000000,
            "LastModified": "2024-01-15T12:00:00Z",
        })
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [FILE_INFO])
        assert result == [FILE_INFO]

    @pytest.mark.asyncio
    async def test_multiple_files_mixed(self) -> None:
        """Two files: one already completed, one new."""
        new_file = {"key": "path/to/new.csv", "size": 100, "last_modified": "2024-06-01T00:00:00Z"}
        completed_file = FILE_INFO

        def mock_get_entity(partition_key: str, row_key: str) -> dict:
            if row_key == FileTrackingStore._row_key(completed_file["key"]):
                return {
                    "Status": "completed",
                    "Size": completed_file["size"],
                    "LastModified": completed_file["last_modified"],
                }
            raise ResourceNotFoundError("Not found")

        table_client = AsyncMock()
        table_client.get_entity = AsyncMock(side_effect=mock_get_entity)
        store = _make_store(table_client)

        result = await store.get_unprocessed_files(SITE_ID, [completed_file, new_file])
        assert result == [new_file]


class TestMarkQueued:
    """mark_queued creates or updates a file tracking entity."""

    @pytest.mark.asyncio
    async def test_creates_new_entity(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_queued(
            site_id=SITE_ID, s3_key=S3_KEY,
            file_size=65000000, last_modified="2024-01-15T12:00:00Z",
            correlation_id="test-corr",
        )

        table_client.create_entity.assert_called_once()
        entity = table_client.create_entity.call_args[0][0]
        assert entity["Status"] == "queued"
        assert entity["FileName"] == "9068_ac_power_data.csv"
        assert entity["Size"] == 65000000

    @pytest.mark.asyncio
    async def test_upserts_on_conflict(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(
            side_effect=ResourceExistsError("Conflict"),
        )
        table_client.update_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_queued(
            site_id=SITE_ID, s3_key=S3_KEY,
            file_size=65000000, last_modified="2024-01-15T12:00:00Z",
            correlation_id="test-corr",
        )

        table_client.update_entity.assert_called_once()


class TestMarkProcessing:
    """mark_processing updates status to processing."""

    @pytest.mark.asyncio
    async def test_updates_status(self) -> None:
        table_client = AsyncMock()
        table_client.upsert_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_processing(SITE_ID, S3_KEY)

        entity = table_client.upsert_entity.call_args[0][0]
        assert entity["Status"] == "processing"


class TestMarkCompleted:
    """mark_completed updates status with dataset metadata."""

    @pytest.mark.asyncio
    async def test_updates_status_and_count(self) -> None:
        table_client = AsyncMock()
        table_client.upsert_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_completed(
            SITE_ID,
            S3_KEY,
            storage_path="pvdaq/site_id=9068/category=ac_power/file.csv",
            file_hash="abc123",
            ingestion_id="ingest-uuid",
            source_url="https://oedi.s3.amazonaws.com/pvdaq/file.csv",
            ingestion_time="2024-01-15T12:00:00+00:00",
        )

        entity = table_client.upsert_entity.call_args[0][0]
        assert entity["Status"] == "completed"
        assert entity["StoragePath"] == "pvdaq/site_id=9068/category=ac_power/file.csv"
        assert entity["FileHash"] == "abc123"
        assert entity["IngestionId"] == "ingest-uuid"
        assert entity["SourceUrl"] == "https://oedi.s3.amazonaws.com/pvdaq/file.csv"
        assert entity["IngestionTime"] == "2024-01-15T12:00:00+00:00"
        assert "CompletedAt" in entity


class TestMarkFailed:
    """mark_failed updates status to failed."""

    @pytest.mark.asyncio
    async def test_updates_status(self) -> None:
        table_client = AsyncMock()
        table_client.upsert_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_failed(SITE_ID, S3_KEY)

        entity = table_client.upsert_entity.call_args[0][0]
        assert entity["Status"] == "failed"


class TestFileTrackingStoreLifecycle:
    """Async context manager lifecycle."""

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        table_client = AsyncMock()
        store = _make_store(table_client)
        async with store as s:
            assert s is store

    @pytest.mark.asyncio
    async def test_injected_client_not_closed(self) -> None:
        table_client = AsyncMock()
        store = FileTrackingStore(
            table_name="t",
            table_service_uri="https://test.table.core.windows.net",
            table_client=table_client,
        )
        await store.close()
        table_client.close.assert_not_called()
