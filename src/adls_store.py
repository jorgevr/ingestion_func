"""ADLS Gen2 streaming upload via the Blob API, with incremental SHA-256 hash (feature 002).

Provides ``AdlsStore`` — uploads a file from a source URL directly to
ADLS Gen2 in a single streaming pass using a staged block-blob upload
(``stage_block`` + ``commit_block_list``). Memory footprint is bounded to
~4 MiB regardless of file size, and the blob only becomes visible once the
block list is committed — a source-side failure (e.g. a 404) leaves no
partial or empty blob behind, because the source download is attempted
before any blob or block is staged.

ADR 0005: the account is ADLS Gen2 (HNS-enabled) but no code path uses a
DFS-only feature (no atomic rename, no ACLs), so storage is accessed
exclusively through the Blob API — one code path for local (Azurite) and
cloud. A ``connection_string`` selects the emulator; otherwise ``account_url``
+ ``DefaultAzureCredential`` is used. Both routes build the same
``BlobServiceClient`` and run the same upload/write methods.
"""

from __future__ import annotations

import hashlib
import json as _json
import logging
from types import TracebackType
from typing import Self

import httpx
from azure.core.exceptions import ResourceExistsError
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobServiceClient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


class AdlsUploadError(Exception):
    """Raised when an ADLS Gen2 upload fails."""


def _block_id(index: int) -> str:
    """Deterministic, fixed-length block ID for ``stage_block``.

    Passed as a plain string — both ``stage_block`` and ``commit_block_list``
    base64-encode the block ID internally, so pre-encoding here would double
    it. Correctness only requires the same raw string be handed to both calls
    for a given block.
    """
    return f"{index:032d}"


class AdlsStore:
    """Streams a remote file into ADLS Gen2 (Blob API) with incremental SHA-256 hashing.

    Uses a staged block-blob upload (``stage_block`` per chunk, then one
    ``commit_block_list``) to keep memory usage bounded at ~4 MiB per upload
    regardless of file size, and so the blob is atomically invisible until
    the whole file has been staged.

    Args:
        account_url: Blob endpoint (e.g. ``https://{account}.blob.core.windows.net``
            in the cloud, or the Azurite blob endpoint locally). Used with
            ``DefaultAzureCredential`` when *connection_string* is not set.
        container_name: Container name (e.g. ``"bronze"``).
        service_client: Optional pre-built ``BlobServiceClient`` for testability.
        connection_string: When set, ``BlobServiceClient.from_connection_string``
            is used instead of *account_url* + credential (local/emulator mode).
    """

    def __init__(
        self,
        account_url: str,
        container_name: str,
        service_client: BlobServiceClient | None = None,
        connection_string: str | None = None,
    ) -> None:
        self._account_url = account_url
        self._container_name = container_name
        self._service_client = service_client
        self._connection_string = connection_string
        self._credential: DefaultAzureCredential | None = None
        self._owns_client = service_client is None

    async def _get_client(self) -> BlobServiceClient:
        """Return a cached BlobServiceClient, built from a connection string
        (local/emulator) or from ``account_url`` + ``DefaultAzureCredential`` (cloud)."""
        if self._service_client is None:
            if self._connection_string:
                self._service_client = BlobServiceClient.from_connection_string(
                    self._connection_string
                )
            else:
                self._credential = DefaultAzureCredential()
                self._service_client = BlobServiceClient(
                    account_url=self._account_url,
                    credential=self._credential,
                )
        return self._service_client

    async def blob_url(self, file_path: str) -> str:
        """Return the blob's canonical URL, as built by the SDK — not hand-assembled.

        ``BlobClient.url`` composes the account endpoint, container and blob
        name (percent-encoding each segment) exactly as the SDK itself would
        parse it back, so the value emitted as ``data.storage_path`` can never
        drift from what the client actually wrote to — whichever credential
        mode built the client.
        """
        blob_svc = await self._get_client()
        blob_client = blob_svc.get_blob_client(
            container=self._container_name, blob=file_path
        )
        return blob_client.url

    async def _ensure_container(self, blob_client: BlobServiceClient) -> None:
        container = blob_client.get_container_client(self._container_name)
        try:
            await container.create_container()
        except ResourceExistsError:
            pass

    async def stream_upload(
        self,
        source_url: str,
        file_path: str,
        http_client: httpx.AsyncClient | None = None,
    ) -> tuple[int, str, int]:
        """Stream a remote file into ADLS Gen2 as a block blob.

        Downloads *source_url* in 4 MiB chunks, staging each chunk as a block
        (``stage_block``) and computing the SHA-256 hash and newline count
        incrementally. The source download is attempted first — nothing is
        created in storage (no container, no staged block) until the response
        headers confirm the source exists — and the blob itself is only
        committed (made visible) via ``commit_block_list`` once the entire
        file has downloaded successfully. A source-side failure therefore
        leaves no blob, partial or otherwise, behind.

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
        owns_http = http_client is None
        if http_client is None:
            http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=10.0, read=300.0),
                follow_redirects=True,
            )

        try:
            hasher = hashlib.sha256()
            offset = 0
            newline_count = 0
            block_ids: list[str] = []

            async with http_client.stream("GET", source_url) as response:
                if response.status_code == 404:
                    raise AdlsUploadError(f"Source not found: {source_url}")
                response.raise_for_status()

                # Only now — with a confirmed-live source — do we touch storage.
                blob_svc = await self._get_client()
                await self._ensure_container(blob_svc)
                blob_client = blob_svc.get_blob_client(
                    container=self._container_name, blob=file_path
                )

                async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
                    hasher.update(chunk)
                    newline_count += chunk.count(b"\n")
                    block_id = _block_id(len(block_ids))
                    await blob_client.stage_block(block_id, chunk)
                    block_ids.append(block_id)
                    offset += len(chunk)

            # Commit — this is the single point at which the blob becomes visible.
            await blob_client.commit_block_list(block_ids)
            file_hash = hasher.hexdigest()

            logger.debug(
                "Uploaded %d bytes to %s/%s (sha256=%s, rows~=%d)",
                offset,
                self._container_name,
                file_path,
                file_hash[:16],
                newline_count,
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
        """Write a JSON-serialisable dict as a single block blob in ADLS Gen2.

        ``upload_blob`` is a single call that creates the block blob
        atomically — it is not visible until the call completes.

        Args:
            file_path: Destination path within the container, without a
                leading slash (e.g. ``"source=pvdaq/.../metadata.json"``).
            data: JSON-serialisable dict to write.

        Raises:
            AdlsUploadError: If the write fails.
        """
        payload = _json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")

        try:
            blob_svc = await self._get_client()
            await self._ensure_container(blob_svc)
            blob_client = blob_svc.get_blob_client(
                container=self._container_name, blob=file_path
            )
            await blob_client.upload_blob(payload, overwrite=True)

            logger.debug(
                "Wrote %d bytes of JSON to %s/%s",
                len(payload),
                self._container_name,
                file_path,
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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()
