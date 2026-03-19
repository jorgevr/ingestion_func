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
        """SHA-256 hash of the S3 key (64 hex chars)."""
        return hashlib.sha256(s3_key.encode()).hexdigest()

    @staticmethod
    def _versioned_row_key(s3_key: str, version: int) -> str:
        """SHA-256 hash of the S3 key + version suffix (e.g. ``…abc_v1``)."""
        return f"{hashlib.sha256(s3_key.encode()).hexdigest()}_v{version}"

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

    async def get_versions(self, site_id: int, s3_key: str) -> list[int]:
        """Return all ingestion version numbers recorded for a given S3 key.

        Queries all entities in the partition for *site_id* whose ``S3Key``
        field matches *s3_key* and returns the ``Version`` integer from each.
        The base queued/processing entity (RowKey without a ``_v{n}`` suffix)
        has no ``Version`` field and is therefore excluded automatically.

        Returns an empty list for a file that has never been ingested.
        """
        client = await self._get_client()
        pk = self._partition_key(site_id)
        # OData filter — single quotes in values must be escaped as ''
        escaped = s3_key.replace("'", "''")
        query = f"PartitionKey eq '{pk}' and S3Key eq '{escaped}'"
        versions: list[int] = []
        async for entity in client.query_entities(query_filter=query):
            v = entity.get("Version")
            if v is not None:
                versions.append(int(v))
        return versions

    async def mark_queued(
        self,
        site_id: int,
        s3_key: str,
        file_size: int,
        last_modified: str,
        correlation_id: str,
        category: str = "",
    ) -> None:
        """Create or update a file tracking entity with status ``queued``.

        Args:
            site_id: PVDAQ site identifier.
            s3_key: Full S3 object key.
            file_size: File size in bytes.
            last_modified: ISO-8601 last-modified from S3.
            correlation_id: Dispatcher correlation ID.
            category: Measurement category extracted from filename.
        """
        client = await self._get_client()
        entity = {
            "PartitionKey": self._partition_key(site_id),
            "RowKey": self._row_key(s3_key),
            "S3Key": s3_key,
            "FileName": PurePosixPath(s3_key).name,
            "Category": category,
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

    async def mark_processing(self, site_id: int, s3_key: str, version: int) -> None:
        """Create a versioned tracking entity with status ``processing``.

        Creates a new entity at the versioned RowKey (``SHA256_v{version}``)
        and updates the base entity so that ``get_unprocessed_files`` reflects
        the current state.
        """
        client = await self._get_client()
        pk = self._partition_key(site_id)
        file_name = PurePosixPath(s3_key).name
        # Versioned entity — immutable record for this ingestion version
        await client.upsert_entity(
            {
                "PartitionKey": pk,
                "RowKey": self._versioned_row_key(s3_key, version),
                "S3Key": s3_key,
                "FileName": file_name,
                "Version": version,
                "Status": "processing",
            },
            mode="merge",
        )
        # Base entity — updated so get_unprocessed_files stays current
        await client.upsert_entity(
            {
                "PartitionKey": pk,
                "RowKey": self._row_key(s3_key),
                "Status": "processing",
            },
            mode="merge",
        )

    async def mark_completed(
        self,
        site_id: int,
        s3_key: str,
        version: int,
        category: str,
        storage_path: str,
        file_hash: str,
        ingestion_id: str,
        source_url: str,
        ingestion_time: str,
        row_count: int,
        metadata_path: str,
    ) -> None:
        """Update tracking entities to status ``completed`` with full metadata.

        Writes all metadata to the versioned entity (immutable record) and
        updates the base entity status so that ``get_unprocessed_files`` can
        detect the completed state on the next dispatcher run.

        Args:
            site_id: PVDAQ site identifier.
            s3_key: Full S3 object key.
            version: Ingestion version number (1 = first ingestion).
            category: Measurement category (e.g. ``"ac_power"``).
            storage_path: ADLS Gen2 path where the CSV was stored.
            file_hash: SHA-256 hex digest of the file content.
            ingestion_id: UUID identifying this ingestion run.
            source_url: Original S3 download URL.
            ingestion_time: ISO-8601 timestamp when the file was stored.
            row_count: Newline count from streaming upload.
            metadata_path: ADLS path of the accompanying ``metadata.json``.
        """
        client = await self._get_client()
        pk = self._partition_key(site_id)
        completed_at = datetime.now(timezone.utc).isoformat()
        # Versioned entity — full metadata record
        await client.upsert_entity(
            {
                "PartitionKey": pk,
                "RowKey": self._versioned_row_key(s3_key, version),
                "S3Key": s3_key,
                "FileName": PurePosixPath(s3_key).name,
                "Category": category,
                "Version": version,
                "Status": "completed",
                "CompletedAt": completed_at,
                "StoragePath": storage_path,
                "FileHash": file_hash,
                "IngestionId": ingestion_id,
                "SourceUrl": source_url,
                "IngestionTime": ingestion_time,
                "RowCount": row_count,
                "MetadataPath": metadata_path,
            },
            mode="merge",
        )
        # Base entity — update status + LastModified so get_unprocessed_files works
        await client.upsert_entity(
            {
                "PartitionKey": pk,
                "RowKey": self._row_key(s3_key),
                "Status": "completed",
                "CompletedAt": completed_at,
            },
            mode="merge",
        )

    async def mark_failed(self, site_id: int, s3_key: str, version: int = 0) -> None:
        """Update tracking entities to status ``failed``.

        Args:
            version: Ingestion version being attempted (0 if unknown).
        """
        client = await self._get_client()
        pk = self._partition_key(site_id)
        if version:
            await client.upsert_entity(
                {
                    "PartitionKey": pk,
                    "RowKey": self._versioned_row_key(s3_key, version),
                    "S3Key": s3_key,
                    "Version": version,
                    "Status": "failed",
                },
                mode="merge",
            )
        await client.upsert_entity(
            {
                "PartitionKey": pk,
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
