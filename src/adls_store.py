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
from azure.core import MatchConditions
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob.aio import BlobClient, BlobServiceClient

logger = logging.getLogger(__name__)

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB


class AdlsUploadError(Exception):
    """Raised when an ADLS Gen2 upload fails."""


class AdlsBlobIdentityMismatchError(AdlsUploadError):
    """Raised when the target blob already exists under a *different*
    source identity (``s3_key``) than the one being uploaded.

    ``_adls_path`` derives its path from ``site_id`` + a *derived* category
    string (via ``extract_category``), not from the S3 key directly — two
    genuinely different source files can map to the same category and
    therefore the same path+version. Without this guard the second upload
    would silently overwrite the first file's data. It is a subclass of
    ``AdlsUploadError`` so the existing deterministic-by-default
    classification (function_app._is_transient) treats it correctly with no
    special-casing: retrying the identical upload would hit the identical
    collision every time.
    """


class AdlsCommitContentionError(AdlsUploadError):
    """Raised when the conditional commit in ``_commit_with_identity_guard``
    loses the race *twice* in a row — not an identity conflict (the re-read
    in between found the *same* s3_key each time, i.e. a same-file retry
    contending with itself), just unusually heavy concurrent write pressure
    on this exact path.

    Unlike ``AdlsBlobIdentityMismatchError`` (a genuine, permanent conflict
    that retrying can never resolve), this is transient: another attempt,
    possibly after a moment's backoff, is likely to land once the
    contention clears. It is still a subclass of ``AdlsUploadError`` (so
    ``stream_upload``'s own ``except AdlsUploadError: raise`` lets it
    through unwrapped), but ``function_app._is_transient_single`` gives it
    an explicit isinstance check rather than classifying it via the
    wrapped-cause walk — a wrapped 409/412 would otherwise read as
    deterministic (see ``_is_transient_single``'s ``HttpResponseError``
    branch), which is wrong specifically for this exhausted-retry case.
    """


_MAX_COMMIT_ATTEMPTS = 2


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

    async def _check_no_identity_collision(
        self, blob_client: BlobClient, file_path: str, s3_key: str
    ) -> None:
        """Cheap pre-check: refuse fast, *before staging a single block*, if
        the target blob already holds a different source identity.

        This is an optimization, not the actual guarantee — two concurrent
        uploads to the same path can both pass this read before either has
        committed, so it cannot close the race on its own. The real,
        race-proof guard is ``_commit_with_identity_guard``'s conditional
        commit; this just avoids paying to stream and stage a whole file
        for an upload that a plain, uncontended check already shows is
        doomed (e.g. two split files that extract to the same
        ``_adls_path``, discovered on an ordinary, non-racing redelivery).

        A no-op when the blob doesn't exist yet, or exists with no recorded
        ``s3_key`` (pre-existing blobs from before this guard existed, or a
        blank string — nothing to compare against either way).
        """
        try:
            props = await blob_client.get_blob_properties()
        except ResourceNotFoundError:
            return
        existing_s3_key = (props.metadata or {}).get("s3_key")
        if existing_s3_key and existing_s3_key != s3_key:
            logger.error(
                "Blob identity collision at %s/%s: existing blob was written "
                "for s3_key=%r, this upload is for s3_key=%r — refusing to "
                "overwrite",
                self._container_name,
                file_path,
                existing_s3_key,
                s3_key,
            )
            raise AdlsBlobIdentityMismatchError(
                f"Refusing to overwrite {file_path}: existing blob's s3_key "
                f"({existing_s3_key!r}) does not match this upload's "
                f"({s3_key!r})"
            )

    async def _commit_with_identity_guard(
        self,
        blob_client: BlobClient,
        file_path: str,
        s3_key: str,
        block_ids: list[str],
    ) -> None:
        """Atomically commit the block list — the race-proof half of the
        identity guard (``_check_no_identity_collision`` is the cheap,
        non-authoritative pre-check; this is what actually closes the race).

        A plain read-then-write check (read metadata, decide, then commit)
        has a TOCTOU race: two concurrent uploads to the same path (e.g. two
        split files that both extract to the same ``_adls_path``) can each
        pass the read and then both commit, one silently overwriting the
        other. This instead makes the *commit itself* conditional:

        - No blob exists yet at this path -> commit with
          ``match_condition=IfMissing`` (If-None-Match: *). If a concurrent
          writer commits first, this fails server-side.
        - A blob exists with this same ``s3_key`` already in its metadata
          (a retry/redelivery re-uploading the identical file, or an empty
          ``s3_key`` — treated as "not recorded", same as absent) -> commit
          with ``etag=<props.etag>, match_condition=IfNotModified``
          (If-Match), so this only succeeds if nothing has changed since
          the read.
        - A blob exists with a *different*, non-empty ``s3_key`` -> refuse
          immediately, no commit attempted.

        On a conditional-commit failure (``ResourceExistsError`` — 409, from
        the ``IfMissing`` branch losing a race — or ``ResourceModifiedError``
        — 412, from the ``IfNotModified`` branch losing a race), the
        properties are re-read once more and the decision is made again: if
        the winner turns out to share this upload's ``s3_key``, this is just
        a lost race against an equivalent retry and the commit is attempted
        once more with the fresh etag; if it's a different ``s3_key``, this
        raises ``AdlsBlobIdentityMismatchError``. If *that* retried commit
        *also* loses the race — real, heavy contention on this exact path
        rather than an identity conflict — this gives up and raises
        ``AdlsCommitContentionError`` (transient: see its docstring) instead
        of exhausting attempts silently or misclassifying the failure as a
        deterministic identity conflict.
        """
        for attempt in range(_MAX_COMMIT_ATTEMPTS):
            try:
                props = await blob_client.get_blob_properties()
            except ResourceNotFoundError:
                existing_s3_key = None
                etag = None
            else:
                existing_s3_key = (props.metadata or {}).get("s3_key")
                etag = props.etag

            if existing_s3_key and existing_s3_key != s3_key:
                logger.error(
                    "Blob identity collision at %s/%s: existing blob was "
                    "written for s3_key=%r, this upload is for s3_key=%r — "
                    "refusing to overwrite",
                    self._container_name,
                    file_path,
                    existing_s3_key,
                    s3_key,
                )
                raise AdlsBlobIdentityMismatchError(
                    f"Refusing to overwrite {file_path}: existing blob's "
                    f"s3_key ({existing_s3_key!r}) does not match this "
                    f"upload's ({s3_key!r})"
                )

            commit_kwargs = (
                {"match_condition": MatchConditions.IfMissing}
                if etag is None
                else {"etag": etag, "match_condition": MatchConditions.IfNotModified}
            )

            try:
                await blob_client.commit_block_list(
                    block_ids, metadata={"s3_key": s3_key}, **commit_kwargs
                )
                return
            except (ResourceExistsError, ResourceModifiedError) as exc:
                if attempt < _MAX_COMMIT_ATTEMPTS - 1:
                    logger.warning(
                        "Conditional commit to %s/%s lost a race — re-reading "
                        "blob identity once before deciding again",
                        self._container_name,
                        file_path,
                    )
                    continue
                logger.error(
                    "Conditional commit to %s/%s lost the race %d times in "
                    "a row against an equal-identity writer — giving up "
                    "(transient: safe to redeliver)",
                    self._container_name,
                    file_path,
                    _MAX_COMMIT_ATTEMPTS,
                )
                raise AdlsCommitContentionError(
                    f"Commit to {file_path} lost the conditional-write race "
                    f"{_MAX_COMMIT_ATTEMPTS} times in a row"
                ) from exc

    async def stream_upload(
        self,
        source_url: str,
        file_path: str,
        s3_key: str,
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

        The identity guard against *s3_key* runs twice, for two different
        reasons: a cheap pre-check (``_check_no_identity_collision``)
        before any block is staged, so an ordinary, non-racing collision is
        rejected without wasting a stream+stage of the whole file; and the
        atomic, race-proof conditional commit (``_commit_with_identity_guard``)
        at the end, which is what actually closes the TOCTOU race between
        two genuinely concurrent uploads to the same path (the pre-check
        alone cannot — both could pass it before either commits). A blank
        (empty-string) recorded ``s3_key`` is treated the same as no
        recorded ``s3_key`` at all — "not recorded" — by both checks. On a
        successful commit, *s3_key* is written to the blob's metadata so a
        *future* upload to the same path can make the same checks.

        Args:
            source_url: HTTP(S) URL to download from (typically an S3 URL).
            file_path: Destination path within the container, without a
                leading slash.
            s3_key: The full S3 object key this upload is for — the true,
                always-unique source identity (unlike the path, which is
                derived from a non-unique category string). Recorded in the
                blob's metadata and used as the identity-collision guard.
            http_client: Optional ``httpx.AsyncClient`` for testability.

        Returns:
            A ``(bytes_written, sha256_hex, newline_count)`` tuple.
            ``newline_count`` is the number of ``\\n`` bytes encountered
            during streaming — a zero-cost row count proxy (no CSV parsing).

        Raises:
            AdlsUploadError: If the download or upload fails.
            AdlsBlobIdentityMismatchError: If the target path already holds a
                blob written for a different ``s3_key``.
            AdlsCommitContentionError: If the conditional commit loses the
                race against an equal-identity writer on every attempt —
                transient, safe to retry.
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
                await self._check_no_identity_collision(blob_client, file_path, s3_key)

                async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
                    hasher.update(chunk)
                    newline_count += chunk.count(b"\n")
                    block_id = _block_id(len(block_ids))
                    await blob_client.stage_block(block_id, chunk)
                    block_ids.append(block_id)
                    offset += len(chunk)

            # Commit — this is the single point at which the blob becomes
            # visible — atomically guarded against a different s3_key
            # already occupying this path (see _commit_with_identity_guard).
            await self._commit_with_identity_guard(
                blob_client, file_path, s3_key, block_ids
            )
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

    async def write_json(self, file_path: str, data: dict, s3_key: str) -> None:
        """Write a JSON-serialisable dict as a single block blob in ADLS Gen2.

        ``upload_blob`` is a single call that creates the block blob
        atomically — it is not visible until the call completes.

        Args:
            file_path: Destination path within the container, without a
                leading slash (e.g. ``"source=pvdaq/.../metadata.json"``).
            data: JSON-serialisable dict to write.
            s3_key: The S3 object key this metadata document describes —
                recorded in the blob's metadata for the same traceability
                stream_upload's blob gets. Unlike the CSV upload, this path
                has no version component (a rerun intentionally overwrites
                metadata.json), so this is not guarded by
                ``_commit_with_identity_guard`` — it is a plain overwrite.

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
            await blob_client.upload_blob(
                payload, overwrite=True, metadata={"s3_key": s3_key}
            )

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
