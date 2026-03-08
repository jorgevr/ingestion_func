"""Idempotency store backed by Azure Table Storage.

Implements a write-before-emit pattern using conditional inserts to
prevent duplicate event emission. See research.md R5 for design rationale.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import PurePosixPath
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.data.tables.aio import TableClient
from azure.identity.aio import DefaultAzureCredential

logger = logging.getLogger(__name__)


class IdempotencyResult(Enum):
    """Result of an idempotency check."""

    NEW = "new"
    DUPLICATE = "duplicate"
    RETRY_EMIT = "retry_emit"


class IdempotencyStore:
    """Azure Table Storage-backed idempotency store.

    Args:
        table_name: Name of the Table Storage table.
        table_service_uri: Table Storage account URI.
        table_client: Optional pre-built ``TableClient`` for testability.
    """

    def __init__(
        self,
        table_name: str,
        table_service_uri: str,
        table_client: TableClient | None = None,
    ) -> None:
        self._table_name = table_name
        self._table_service_uri = table_service_uri
        self._table_client = table_client
        self._credential: DefaultAzureCredential | None = None
        self._owns_client = table_client is None

    async def _get_client(self) -> TableClient:
        if self._table_client is None:
            self._credential = DefaultAzureCredential()
            self._table_client = TableClient(
                endpoint=self._table_service_uri,
                table_name=self._table_name,
                credential=self._credential,
            )
        return self._table_client

    @staticmethod
    def _partition_key(measdatetime: str) -> str:
        """Derive a date-based PartitionKey from the measurement datetime."""
        return measdatetime[:10]  # "YYYY-MM-DD"

    @staticmethod
    def _row_key(site_id: int, measdatetime: str) -> str:
        """Derive a composite RowKey from site ID and measurement datetime."""
        return f"{site_id}_{measdatetime}"

    @staticmethod
    def _row_key_historical(site_id: int, file_name: str, measdatetime: str) -> str:
        """Derive a composite RowKey for historical records.

        Uses the filename stem as a category discriminator to avoid
        collisions between different CSV files for the same site and
        timestamp (e.g. ac_power vs environment).
        """
        stem = PurePosixPath(file_name).stem
        return f"{site_id}_{stem}_{measdatetime}"

    async def _check_and_reserve(
        self,
        pk: str,
        rk: str,
        correlation_id: str,
    ) -> IdempotencyResult:
        """Core idempotency check: conditional insert then inspect on conflict.

        Args:
            pk: Table Storage PartitionKey.
            rk: Table Storage RowKey.
            correlation_id: Current invocation correlation ID.

        Returns:
            ``NEW`` if the record is new and has been reserved,
            ``DUPLICATE`` if the record was already processed,
            ``RETRY_EMIT`` if a previous attempt crashed before completing.
        """
        client = await self._get_client()
        entity = {
            "PartitionKey": pk,
            "RowKey": rk,
            "Status": "pending",
            "CorrelationId": correlation_id,
            "CreatedAt": datetime.now(timezone.utc).isoformat(),
        }

        try:
            await client.create_entity(entity)
            return IdempotencyResult.NEW
        except ResourceExistsError:
            existing = await client.get_entity(partition_key=pk, row_key=rk)
            if existing.get("Status") == "completed":
                return IdempotencyResult.DUPLICATE
            return IdempotencyResult.RETRY_EMIT

    async def check_and_reserve(
        self,
        site_id: int,
        measdatetime: str,
        correlation_id: str,
    ) -> IdempotencyResult:
        """Check whether a record has been processed and reserve it if new.

        Args:
            site_id: PVDAQ site identifier.
            measdatetime: ISO-8601 measurement datetime string.
            correlation_id: Current invocation correlation ID.
        """
        pk = self._partition_key(measdatetime)
        rk = self._row_key(site_id, measdatetime)
        return await self._check_and_reserve(pk, rk, correlation_id)

    async def check_and_reserve_historical(
        self,
        site_id: int,
        file_name: str,
        measdatetime: str,
        correlation_id: str,
    ) -> IdempotencyResult:
        """Check and reserve a historical record using the file-aware key.

        Same semantics as ``check_and_reserve`` but uses a composite key
        that includes the file name stem to prevent collisions between
        different CSV categories for the same site/timestamp.

        Args:
            site_id: PVDAQ site identifier.
            file_name: CSV file name (stem used as discriminator).
            measdatetime: ISO-8601 measurement datetime string.
            correlation_id: Current invocation correlation ID.
        """
        pk = self._partition_key(measdatetime)
        rk = self._row_key_historical(site_id, file_name, measdatetime)
        return await self._check_and_reserve(pk, rk, correlation_id)

    async def mark_completed(
        self,
        partition_key: str,
        row_key: str,
    ) -> None:
        """Mark an idempotency record as completed after successful emission.

        Args:
            partition_key: Table Storage PartitionKey (date string).
            row_key: Table Storage RowKey (composite ``{SiteID}_{measdatetime}``).
        """
        client = await self._get_client()
        await client.update_entity(
            {
                "PartitionKey": partition_key,
                "RowKey": row_key,
                "Status": "completed",
                "CompletedAt": datetime.now(timezone.utc).isoformat(),
            },
            mode="merge",
        )

    async def mark_completed_for(
        self,
        site_id: int,
        measdatetime: str,
    ) -> None:
        """Mark a daily record as completed using domain arguments.

        Encapsulates key derivation so callers don't access private methods.
        """
        pk = self._partition_key(measdatetime)
        rk = self._row_key(site_id, measdatetime)
        await self.mark_completed(pk, rk)

    async def mark_completed_for_historical(
        self,
        site_id: int,
        file_name: str,
        measdatetime: str,
    ) -> None:
        """Mark a historical record as completed using domain arguments.

        Encapsulates key derivation so callers don't access private methods.
        """
        pk = self._partition_key(measdatetime)
        rk = self._row_key_historical(site_id, file_name, measdatetime)
        await self.mark_completed(pk, rk)

    async def cleanup_expired(self, ttl_days: int) -> int:
        """Delete idempotency records older than ``ttl_days``.

        Queries partitions with a PartitionKey earlier than the cutoff
        date and deletes them in batches.

        Args:
            ttl_days: Number of days after which records are considered expired.

        Returns:
            Number of entities deleted.

        Note:
            This method should be called from a separate daily timer function
            or manual invocation. The timer trigger registration is deferred
            to a follow-on task.
        """
        client = await self._get_client()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).strftime("%Y-%m-%d")
        deleted = 0

        query = f"PartitionKey lt '{cutoff}'"
        async for entity in client.query_entities(query_filter=query):
            await client.delete_entity(
                partition_key=entity["PartitionKey"],
                row_key=entity["RowKey"],
            )
            deleted += 1

        logger.info("Idempotency cleanup: deleted %d expired entities (cutoff=%s)", deleted, cutoff)
        return deleted

    async def close(self) -> None:
        """Close the underlying client and credential if owned."""
        if self._table_client is not None and self._owns_client:
            await self._table_client.close()
        if self._credential is not None:
            await self._credential.close()

    async def __aenter__(self) -> IdempotencyStore:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
