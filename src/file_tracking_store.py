"""File tracking store backed by Azure Table Storage (feature 002).

Tracks discovery and processing status of each CSV file from the OEDI
S3 bucket.  Used by the historical dispatcher to skip already-processed
files and by the worker to record progress.

Entity schema matches ``contracts/file-tracking-entity.json``.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.data.tables.aio import TableClient
from azure.identity.aio import DefaultAzureCredential

logger = logging.getLogger(__name__)


class FileTrackingStore:
    """Azure Table Storage-backed file tracking store.

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
    def _partition_key(site_id: int) -> str:
        return str(site_id)

    @staticmethod
    def _row_key(s3_key: str) -> str:
        """SHA-256 hash of the S3 key, truncated to 64 characters."""
        return hashlib.sha256(s3_key.encode()).hexdigest()[:64]

    async def get_unprocessed_files(
        self,
        site_id: int,
        discovered_files: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Compare S3 listing against tracked files and return new/changed ones.

        Args:
            site_id: PVDAQ site identifier.
            discovered_files: List of dicts from ``OediHistoricalClient.list_csv_files()``
                with keys ``key``, ``size``, ``last_modified``.

        Returns:
            Subset of *discovered_files* that are new or have changed
            (size or last_modified differs from tracked entity).
        """
        client = await self._get_client()
        pk = self._partition_key(site_id)
        unprocessed: list[dict[str, Any]] = []

        for file_info in discovered_files:
            rk = self._row_key(file_info["key"])
            try:
                existing = await client.get_entity(partition_key=pk, row_key=rk)
                # File already tracked — check if it changed
                if (
                    existing.get("Status") == "completed"
                    and existing.get("Size") == file_info["size"]
                    and existing.get("LastModified") == file_info.get("last_modified", "")
                ):
                    continue
                # Changed or not completed — reprocess
                unprocessed.append(file_info)
            except ResourceNotFoundError:
                # New file — not tracked yet
                unprocessed.append(file_info)

        return unprocessed

    async def mark_queued(
        self,
        site_id: int,
        s3_key: str,
        file_size: int,
        last_modified: str,
        correlation_id: str,
    ) -> None:
        """Create or update a file tracking entity with status ``queued``.

        Args:
            site_id: PVDAQ site identifier.
            s3_key: Full S3 object key.
            file_size: File size in bytes.
            last_modified: ISO-8601 last-modified from S3.
            correlation_id: Dispatcher correlation ID.
        """
        client = await self._get_client()
        entity = {
            "PartitionKey": self._partition_key(site_id),
            "RowKey": self._row_key(s3_key),
            "S3Key": s3_key,
            "FileName": PurePosixPath(s3_key).name,
            "Size": file_size,
            "LastModified": last_modified,
            "Status": "queued",
            "EnqueuedAt": datetime.now(timezone.utc).isoformat(),
            "CorrelationId": correlation_id,
        }
        try:
            await client.create_entity(entity)
        except ResourceExistsError:
            await client.update_entity(entity, mode="replace")

    async def mark_processing(self, site_id: int, s3_key: str) -> None:
        """Update a file tracking entity to status ``processing``.

        Uses upsert so the worker is resilient to missing entities
        (e.g. stale queue messages from a previous dispatcher run).
        """
        client = await self._get_client()
        await client.upsert_entity(
            {
                "PartitionKey": self._partition_key(site_id),
                "RowKey": self._row_key(s3_key),
                "S3Key": s3_key,
                "FileName": PurePosixPath(s3_key).name,
                "Status": "processing",
            },
            mode="merge",
        )

    async def mark_completed(
        self, site_id: int, s3_key: str, records_emitted: int,
    ) -> None:
        """Update a file tracking entity to status ``completed``."""
        client = await self._get_client()
        await client.upsert_entity(
            {
                "PartitionKey": self._partition_key(site_id),
                "RowKey": self._row_key(s3_key),
                "Status": "completed",
                "CompletedAt": datetime.now(timezone.utc).isoformat(),
                "RecordsEmitted": records_emitted,
            },
            mode="merge",
        )

    async def mark_failed(self, site_id: int, s3_key: str) -> None:
        """Update a file tracking entity to status ``failed``."""
        client = await self._get_client()
        await client.upsert_entity(
            {
                "PartitionKey": self._partition_key(site_id),
                "RowKey": self._row_key(s3_key),
                "Status": "failed",
            },
            mode="merge",
        )

    async def close(self) -> None:
        """Close the underlying client and credential if owned."""
        if self._table_client is not None and self._owns_client:
            await self._table_client.close()
        if self._credential is not None:
            await self._credential.close()

    async def __aenter__(self) -> FileTrackingStore:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
