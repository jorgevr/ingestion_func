"""Unit tests for the idempotency store (T028).

Tests cover: new record reserve, duplicate detection, pending record retry,
mark_completed, composite key format, and date-based PartitionKey.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.idempotency_store import IdempotencyResult, IdempotencyStore


def _make_store(table_client: AsyncMock | None = None) -> IdempotencyStore:
    """Create an IdempotencyStore with a mock TableClient."""
    return IdempotencyStore(
        table_name="PvdaqIdempotency",
        table_service_uri="https://test.table.core.windows.net",
        table_client=table_client,
    )


class TestIdempotencyResult:
    """IdempotencyResult enum has expected values."""

    def test_enum_values(self) -> None:
        assert IdempotencyResult.NEW.value == "new"
        assert IdempotencyResult.DUPLICATE.value == "duplicate"
        assert IdempotencyResult.RETRY_EMIT.value == "retry_emit"


class TestCheckAndReserveNew:
    """New record: create_entity succeeds → returns NEW."""

    @pytest.mark.asyncio
    async def test_new_record_returns_new(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="test-correlation-id",
        )

        assert result == IdempotencyResult.NEW
        table_client.create_entity.assert_called_once()
        entity = table_client.create_entity.call_args[0][0]
        assert entity["Status"] == "pending"


class TestDuplicateDetection:
    """Duplicate record: create_entity raises ResourceExistsError with completed status."""

    @pytest.mark.asyncio
    async def test_completed_duplicate_returns_duplicate(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(side_effect=ResourceExistsError("Entity already exists"))
        table_client.get_entity = AsyncMock(return_value={"Status": "completed"})
        store = _make_store(table_client)

        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="test-correlation-id",
        )

        assert result == IdempotencyResult.DUPLICATE


class TestPendingRetry:
    """Pending record (previous crash): returns RETRY_EMIT."""

    @pytest.mark.asyncio
    async def test_pending_record_returns_retry(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(side_effect=ResourceExistsError("Entity already exists"))
        table_client.get_entity = AsyncMock(return_value={"Status": "pending"})
        store = _make_store(table_client)

        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="test-correlation-id",
        )

        assert result == IdempotencyResult.RETRY_EMIT


class TestMarkCompleted:
    """mark_completed calls update_entity with Status=completed."""

    @pytest.mark.asyncio
    async def test_updates_entity_status(self) -> None:
        table_client = AsyncMock()
        table_client.update_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.mark_completed(
            partition_key="2026-01-15",
            row_key="2_2026-01-15T12:00:00",
        )

        table_client.update_entity.assert_called_once()
        entity = table_client.update_entity.call_args[0][0]
        assert entity["Status"] == "completed"
        assert entity["PartitionKey"] == "2026-01-15"
        assert entity["RowKey"] == "2_2026-01-15T12:00:00"


class TestCompositeKey:
    """Composite key format is '{SiteID}_{measdatetime_iso}'."""

    @pytest.mark.asyncio
    async def test_row_key_format(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.check_and_reserve(
            site_id=34,
            measdatetime="2026-02-20T08:30:00",
            correlation_id="test-correlation-id",
        )

        entity = table_client.create_entity.call_args[0][0]
        assert entity["RowKey"] == "34_2026-02-20T08:30:00"


class TestPartitionKey:
    """PartitionKey is date-based 'YYYY-MM-DD'."""

    @pytest.mark.asyncio
    async def test_partition_key_is_date(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="test-correlation-id",
        )

        entity = table_client.create_entity.call_args[0][0]
        assert entity["PartitionKey"] == "2026-01-15"


class TestCleanupExpired:
    """cleanup_expired deletes entities older than the TTL cutoff."""

    @pytest.mark.asyncio
    async def test_deletes_expired_entities(self) -> None:
        expired_entities = [
            {"PartitionKey": "2025-12-01", "RowKey": "2_2025-12-01T12:00:00"},
            {"PartitionKey": "2025-12-02", "RowKey": "34_2025-12-02T08:00:00"},
        ]

        table_client = AsyncMock()
        # query_entities returns an async iterable directly (not a coroutine)
        table_client.query_entities = MagicMock(return_value=_AsyncIter(expired_entities))
        table_client.delete_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        deleted = await store.cleanup_expired(ttl_days=30)

        assert deleted == 2
        assert table_client.delete_entity.call_count == 2

    @pytest.mark.asyncio
    async def test_returns_zero_when_nothing_expired(self) -> None:
        table_client = AsyncMock()
        table_client.query_entities = MagicMock(return_value=_AsyncIter([]))
        table_client.delete_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        deleted = await store.cleanup_expired(ttl_days=30)

        assert deleted == 0
        table_client.delete_entity.assert_not_called()


class TestIdempotencyContextManager:
    """IdempotencyStore async context manager lifecycle."""

    @pytest.mark.asyncio
    async def test_context_manager_with_injected_client(self) -> None:
        """Injected table_client is not closed (not owned)."""
        table_client = AsyncMock()
        table_client.close = AsyncMock(return_value=None)

        async with IdempotencyStore(
            table_name="PvdaqIdempotency",
            table_service_uri="https://test.table.core.windows.net",
            table_client=table_client,
        ) as store:
            assert store is not None

        table_client.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_client_creates_credential(self) -> None:
        """_get_client creates DefaultAzureCredential + TableClient when no client injected.
        Mocked: Azure Table Storage SDK requires real Azure credentials.
        """
        from unittest.mock import patch

        mock_cred = MagicMock()
        mock_table_client = MagicMock()

        with patch("src.idempotency_store.DefaultAzureCredential", return_value=mock_cred), \
             patch("src.idempotency_store.TableClient", return_value=mock_table_client):
            store = IdempotencyStore(
                table_name="PvdaqIdempotency",
                table_service_uri="https://test.table.core.windows.net",
            )
            client = await store._get_client()

        assert client is mock_table_client
        assert store._credential is mock_cred

    @pytest.mark.asyncio
    async def test_close_owned_client(self) -> None:
        """Owned client and credential are closed."""
        store = IdempotencyStore(
            table_name="PvdaqIdempotency",
            table_service_uri="https://test.table.core.windows.net",
        )
        mock_client = AsyncMock()
        mock_client.close = AsyncMock(return_value=None)
        mock_cred = AsyncMock()
        mock_cred.close = AsyncMock(return_value=None)
        store._table_client = mock_client
        store._credential = mock_cred

        await store.close()

        mock_client.close.assert_called_once()
        mock_cred.close.assert_called_once()


class TestHistoricalRowKey:
    """_row_key_historical includes file stem as category discriminator."""

    def test_includes_file_stem(self) -> None:
        rk = IdempotencyStore._row_key_historical(9068, "9068_ac_power_data.csv", "2023-01-01T00:00:00")
        assert rk == "9068_9068_ac_power_data_2023-01-01T00:00:00"

    def test_different_files_different_keys(self) -> None:
        rk1 = IdempotencyStore._row_key_historical(9068, "9068_ac_power_data.csv", "2023-01-01T00:00:00")
        rk2 = IdempotencyStore._row_key_historical(9068, "9068_environment_data.csv", "2023-01-01T00:00:00")
        assert rk1 != rk2

    def test_strips_csv_extension(self) -> None:
        rk = IdempotencyStore._row_key_historical(9068, "9068_ac_power_data.csv", "2023-01-01T00:00:00")
        assert ".csv" not in rk


class TestCheckAndReserveHistorical:
    """check_and_reserve_historical uses the file-aware key format."""

    @pytest.mark.asyncio
    async def test_new_historical_record_returns_new(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        result = await store.check_and_reserve_historical(
            site_id=9068,
            file_name="9068_ac_power_data.csv",
            measdatetime="2023-01-01T00:00:00",
            correlation_id="test-corr",
        )

        assert result == IdempotencyResult.NEW
        entity = table_client.create_entity.call_args[0][0]
        assert entity["RowKey"] == "9068_9068_ac_power_data_2023-01-01T00:00:00"

    @pytest.mark.asyncio
    async def test_duplicate_historical_returns_duplicate(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(side_effect=ResourceExistsError("exists"))
        table_client.get_entity = AsyncMock(return_value={"Status": "completed"})
        store = _make_store(table_client)

        result = await store.check_and_reserve_historical(
            site_id=9068,
            file_name="9068_ac_power_data.csv",
            measdatetime="2023-01-01T00:00:00",
            correlation_id="test-corr",
        )

        assert result == IdempotencyResult.DUPLICATE

    @pytest.mark.asyncio
    async def test_pending_historical_returns_retry(self) -> None:
        from azure.core.exceptions import ResourceExistsError

        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(side_effect=ResourceExistsError("exists"))
        table_client.get_entity = AsyncMock(return_value={"Status": "pending"})
        store = _make_store(table_client)

        result = await store.check_and_reserve_historical(
            site_id=9068,
            file_name="9068_ac_power_data.csv",
            measdatetime="2023-01-01T00:00:00",
            correlation_id="test-corr",
        )

        assert result == IdempotencyResult.RETRY_EMIT


class _AsyncIter:
    """Helper to simulate async iteration over a list for query_entities mocking."""

    def __init__(self, items: list) -> None:
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration
