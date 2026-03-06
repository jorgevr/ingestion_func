"""OEDI Data Lake client for PVDAQ historical CSV files (feature 002).

Lists and streams CSV files from the 2023-solar-data-prize prefix on
the public S3 bucket at https://oedi-data-lake.s3.amazonaws.com.

Key differences from the feature 001 ``OediDataLakeClient``:
- Uses S3 ListObjectsV2 XML API to discover files
- Streams CSV rows via chunked byte download (bounded memory)
- Handles pagination for large site listings
"""

from __future__ import annotations

import asyncio
import csv
import logging
import xml.etree.ElementTree as ET
from collections.abc import AsyncIterator
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.0

# S3 XML namespace for ListObjectsV2 responses
_S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


class OediHistoricalAccessError(Exception):
    """Raised when an OEDI historical data request fails after all retries."""


class OediHistoricalClient:
    """Async client for listing and streaming PVDAQ historical CSV files.

    Args:
        bucket_url: Base URL of the OEDI S3 bucket.
        historical_prefix: S3 prefix for historical data
            (e.g. ``pvdaq/2023-solar-data-prize``).
        http_client: Optional pre-built ``httpx.AsyncClient`` for testability.
    """

    def __init__(
        self,
        bucket_url: str = "https://oedi-data-lake.s3.amazonaws.com",
        historical_prefix: str = "pvdaq/2023-solar-data-prize",
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._bucket_url = bucket_url.rstrip("/")
        self._historical_prefix = historical_prefix.rstrip("/")
        self._http_client = http_client
        self._owns_client = http_client is None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(10.0, connect=10.0, read=300.0),
                follow_redirects=True,
            )
        return self._http_client

    async def _get_with_retry(self, url: str, **kwargs: Any) -> httpx.Response:
        """GET with exponential backoff on 5xx/timeout errors."""
        client = await self._get_client()
        last_error: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = await client.get(url, **kwargs)

                if response.status_code == 404:
                    return response

                if response.status_code >= 500:
                    backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    logger.warning(
                        "OEDI S3 error %d for %s, retrying in %.1fs (attempt %d/%d)",
                        response.status_code, url, backoff, attempt, MAX_RETRIES,
                    )
                    last_error = OediHistoricalAccessError(
                        f"HTTP {response.status_code} from {url}"
                    )
                    if attempt < MAX_RETRIES:
                        await asyncio.sleep(backoff)
                    continue

                response.raise_for_status()
                return response

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as exc:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "OEDI connection error for %s: %s, retrying in %.1fs (attempt %d/%d)",
                    url, exc, backoff, attempt, MAX_RETRIES,
                )
                last_error = exc
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(backoff)

        raise OediHistoricalAccessError(
            f"OEDI request failed for {url} after {MAX_RETRIES} retries: {last_error}"
        )

    async def list_csv_files(self, site_id: int) -> list[dict[str, Any]]:
        """List all CSV files in a site's data folder via S3 ListObjectsV2.

        Args:
            site_id: PVDAQ site identifier.

        Returns:
            List of dicts with ``key``, ``size``, and ``last_modified``
            for each CSV file found.

        Raises:
            OediHistoricalAccessError: If the listing fails after retries.
        """
        prefix = f"{self._historical_prefix}/{site_id}_OEDI/data/"
        files: list[dict[str, Any]] = []
        continuation_token: str | None = None

        while True:
            params: dict[str, str] = {
                "list-type": "2",
                "prefix": prefix,
            }
            if continuation_token:
                params["continuation-token"] = continuation_token

            url = self._bucket_url
            response = await self._get_with_retry(url, params=params)

            if response.status_code == 404:
                logger.warning("Site %d data folder not found at prefix %s", site_id, prefix)
                return []

            root = ET.fromstring(response.text)

            for contents in root.findall(f"{{{_S3_NS}}}Contents"):
                key_el = contents.find(f"{{{_S3_NS}}}Key")
                size_el = contents.find(f"{{{_S3_NS}}}Size")
                modified_el = contents.find(f"{{{_S3_NS}}}LastModified")

                if key_el is None or key_el.text is None:
                    continue

                key = key_el.text
                if not key.endswith(".csv"):
                    continue

                files.append({
                    "key": key,
                    "size": int(size_el.text) if size_el is not None and size_el.text else 0,
                    "last_modified": modified_el.text if modified_el is not None else "",
                })

            # Check for pagination
            is_truncated_el = root.find(f"{{{_S3_NS}}}IsTruncated")
            if is_truncated_el is not None and is_truncated_el.text == "true":
                next_token_el = root.find(f"{{{_S3_NS}}}NextContinuationToken")
                if next_token_el is not None and next_token_el.text:
                    continuation_token = next_token_el.text
                    continue

            break

        return files

    async def stream_csv_rows(self, s3_key: str) -> AsyncIterator[dict[str, str]]:
        """Stream CSV rows from an S3 object with bounded memory.

        Downloads the file in chunks and yields one dict per CSV row.
        Memory usage is bounded to O(chunk_size + one_line_length).

        Args:
            s3_key: Full S3 object key for the CSV file.

        Yields:
            One dict per CSV data row (header → value mapping).

        Raises:
            OediHistoricalAccessError: If the download fails after retries.
        """
        url = f"{self._bucket_url}/{s3_key}"
        client = await self._get_client()
        last_error: Exception | None = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with client.stream("GET", url) as response:
                    if response.status_code == 404:
                        logger.info("CSV file not found: %s", s3_key)
                        return

                    if response.status_code >= 500:
                        last_error = OediHistoricalAccessError(
                            f"HTTP {response.status_code} from {url}"
                        )
                        if attempt < MAX_RETRIES:
                            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                            await asyncio.sleep(backoff)
                        continue

                    response.raise_for_status()

                    fieldnames: list[str] | None = None
                    remainder = ""

                    async for chunk in response.aiter_bytes(chunk_size=65536):
                        text = remainder + chunk.decode("utf-8", errors="replace")
                        lines = text.split("\n")
                        remainder = lines.pop()  # last incomplete line

                        for line in lines:
                            line = line.rstrip("\r")
                            if not line:
                                continue

                            if fieldnames is None:
                                parsed = list(csv.reader([line]))
                                if parsed:
                                    fieldnames = parsed[0]
                                continue

                            parsed = list(csv.reader([line]))
                            if parsed and parsed[0]:
                                yield dict(zip(fieldnames, parsed[0]))

                    # Handle final remainder line
                    if remainder.strip() and fieldnames is not None:
                        parsed = list(csv.reader([remainder]))
                        if parsed and parsed[0]:
                            yield dict(zip(fieldnames, parsed[0]))

                    return  # Success — exit retry loop

            except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as exc:
                last_error = exc
                if attempt < MAX_RETRIES:
                    backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                    logger.warning(
                        "Stream error for %s: %s, retrying in %.1fs (attempt %d/%d)",
                        url, exc, backoff, attempt, MAX_RETRIES,
                    )
                    await asyncio.sleep(backoff)

        raise OediHistoricalAccessError(
            f"Stream failed for {url} after {MAX_RETRIES} retries: {last_error}"
        )

    async def close(self) -> None:
        """Close the underlying HTTP client if owned by this instance."""
        if self._http_client is not None and self._owns_client:
            await self._http_client.aclose()

    async def __aenter__(self) -> OediHistoricalClient:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
