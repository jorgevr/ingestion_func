"""ADLS Gen2 streaming upload with incremental SHA-256 hash (feature 002).

Provides ``AdlsStore`` — uploads a file from a source URL directly to
ADLS Gen2 in a single streaming pass using the manual
create → append → flush pattern.  Memory footprint is bounded to
~4 MiB regardless of file size.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

import httpx
from azure.identity.aio import DefaultAzureCredential
from azure.storage.filedatalake.aio import DataLakeServiceClient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


class AdlsUploadError(Exception):
    """Raised when an ADLS Gen2 upload fails."""


class AdlsStore:
    """Streams a remote file into ADLS Gen2 with incremental SHA-256 hashing.

    Uses the manual ``create_file`` → ``append_data`` → ``flush_data`` pattern
    to keep memory usage bounded at ~4 MiB per upload regardless of file size.

    Args:
        account_url: ADLS Gen2 DFS endpoint
            (e.g. ``https://{account}.dfs.core.windows.net``).
        container_name: File-system / container name (e.g. ``"raw"``).
        service_client: Optional pre-built ``DataLakeServiceClient`` for
            testability.
    """

    def __init__(
        self,
        account_url: str,
        container_name: str,
        service_client: DataLakeServiceClient | None = None,
    ) -> None:
        self._account_url = account_url
        self._container_name = container_name
        self._service_client = service_client
        self._credential: DefaultAzureCredential | None = None
        self._owns_client = service_client is None

    async def _get_service_client(self) -> DataLakeServiceClient:
        if self._service_client is None:
            self._credential = DefaultAzureCredential()
            self._service_client = DataLakeServiceClient(
                account_url=self._account_url,
                credential=self._credential,
            )
        return self._service_client

    async def stream_upload(
        self,
        source_url: str,
        file_path: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> tuple[int, str, int]:
        """Stream a remote file into ADLS Gen2.

        Downloads *source_url* in 4 MiB chunks, appends each chunk to the
        ADLS file, and computes the SHA-256 hash and newline count
        incrementally.  A single ``flush_data`` call at the end makes the
        file visible.

        Args:
            source_url: HTTP(S) URL to download from (typically an S3 URL).
            file_path: Destination path within the container, without a
                leading slash.
            http_client: Optional ``httpx.AsyncClient`` for testability.

        Returns:
            A ``(bytes_written, sha256_hex, newline_count)`` tuple.
            ``newline_count`` is the number of ``\\n`` bytes encountered
            during streaming — a zero-cost row count proxy (no CSV parsing).

        Raises:
            AdlsUploadError: If the download or upload fails.
        """
        service = await self._get_service_client()
        fs_client = service.get_file_system_client(self._container_name)
        file_client = fs_client.get_file_client(file_path)

        owns_http = http_client is None
        if http_client is None:
            http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=10.0, read=300.0),
                follow_redirects=True,
            )

        try:
            await file_client.create_file()

            hasher = hashlib.sha256()
            offset = 0
            newline_count = 0

            async with http_client.stream("GET", source_url) as response:
                if response.status_code == 404:
                    raise AdlsUploadError(f"Source not found: {source_url}")
                response.raise_for_status()

                async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
                    hasher.update(chunk)
                    newline_count += chunk.count(b"\n")
                    length = len(chunk)
                    await file_client.append_data(
                        data=chunk, offset=offset, length=length,
                    )
                    offset += length

            await file_client.flush_data(offset)
            file_hash = hasher.hexdigest()

            logger.debug(
                "Uploaded %d bytes to %s/%s (sha256=%s, rows~=%d)",
                offset, self._container_name, file_path, file_hash[:16], newline_count,
            )
            return offset, file_hash, newline_count

        except AdlsUploadError:
            raise
        except Exception as exc:
            raise AdlsUploadError(
                f"Failed to upload {source_url} → {file_path}: {exc}"
            ) from exc
        finally:
            if owns_http:
                await http_client.aclose()

    async def write_json(self, file_path: str, data: dict) -> None:
        """Write a JSON-serialisable dict as a single file in ADLS Gen2.

        Uses the same ``create_file → append_data → flush_data`` pattern as
        ``stream_upload`` but as a single chunk (no streaming required since
        ``metadata.json`` is always small, < 2 KB).

        Args:
            file_path: Destination path within the container, without a
                leading slash (e.g. ``"source=pvdaq/.../metadata.json"``).
            data: JSON-serialisable dict to write.

        Raises:
            AdlsUploadError: If the write fails.
        """
        import json as _json

        try:
            service = await self._get_service_client()
            fs_client = service.get_file_system_client(self._container_name)
            file_client = fs_client.get_file_client(file_path)

            payload = _json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
            await file_client.create_file()
            await file_client.append_data(data=payload, offset=0, length=len(payload))
            await file_client.flush_data(len(payload))

            logger.debug(
                "Wrote %d bytes of JSON to %s/%s",
                len(payload), self._container_name, file_path,
            )
        except Exception as exc:
            raise AdlsUploadError(
                f"Failed to write JSON to {file_path}: {exc}"
            ) from exc

    async def close(self) -> None:
        """Close the service client and credential if owned by this instance."""
        if self._service_client is not None and self._owns_client:
            await self._service_client.close()
        if self._credential is not None:
            await self._credential.close()

    async def __aenter__(self) -> AdlsStore:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
