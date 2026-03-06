"""Idempotency integration tests (T031).

Tests cover: first submission emitted + idempotency record created,
second submission (same key) not emitted + logged as duplicate,
pending record (simulated crash) re-emitted + marked completed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from azure.core.exceptions import ResourceExistsError

from src.idempotency_store import IdempotencyResult, IdempotencyStore


def _make_store(table_client: AsyncMock) -> IdempotencyStore:
    return IdempotencyStore(
        table_name="PvdaqIdempotency",
        table_service_uri="https://test.table.core.windows.net",
        table_client=table_client,
    )


class TestFirstSubmissionEmitted:
    """First submission: record is new, emitted, and marked completed."""

    @pytest.mark.asyncio
    async def test_new_record_flow(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(return_value=None)
        table_client.update_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        # Step 1: check_and_reserve returns NEW
        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="corr-001",
        )
        assert result == IdempotencyResult.NEW
        table_client.create_entity.assert_called_once()

        # Step 2: After emission, mark_completed
        await store.mark_completed(
            partition_key="2026-01-15",
            row_key="2_2026-01-15T12:00:00",
        )
        table_client.update_entity.assert_called_once()
        entity = table_client.update_entity.call_args[0][0]
        assert entity["Status"] == "completed"


class TestDuplicateNotEmitted:
    """Second submission (same key): not emitted, logged as duplicate."""

    @pytest.mark.asyncio
    async def test_duplicate_skipped(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(
            side_effect=ResourceExistsError("Entity already exists")
        )
        table_client.get_entity = AsyncMock(return_value={"Status": "completed"})
        store = _make_store(table_client)

        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="corr-002",
        )

        assert result == IdempotencyResult.DUPLICATE
        # get_entity called to check existing status
        table_client.get_entity.assert_called_once()


class TestPendingReEmitted:
    """Pending record (simulated crash): re-emitted + marked completed."""

    @pytest.mark.asyncio
    async def test_pending_retried(self) -> None:
        table_client = AsyncMock()
        table_client.create_entity = AsyncMock(
            side_effect=ResourceExistsError("Entity already exists")
        )
        table_client.get_entity = AsyncMock(return_value={"Status": "pending"})
        table_client.update_entity = AsyncMock(return_value=None)
        store = _make_store(table_client)

        # Step 1: check_and_reserve returns RETRY_EMIT
        result = await store.check_and_reserve(
            site_id=2,
            measdatetime="2026-01-15T12:00:00",
            correlation_id="corr-003",
        )
        assert result == IdempotencyResult.RETRY_EMIT

        # Step 2: Re-emit, then mark_completed
        await store.mark_completed(
            partition_key="2026-01-15",
            row_key="2_2026-01-15T12:00:00",
        )
        table_client.update_entity.assert_called_once()
        entity = table_client.update_entity.call_args[0][0]
        assert entity["Status"] == "completed"
