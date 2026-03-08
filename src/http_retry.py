"""Shared HTTP retry helper with exponential backoff.

Used by both OediDataLakeClient (feature 001) and OediHistoricalClient
(feature 002) to avoid duplicating retry logic.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 1.0


async def get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    *,
    error_class: type[Exception] = Exception,
    **kwargs: Any,
) -> httpx.Response:
    """GET a URL with exponential backoff on 5xx/timeout errors.

    Args:
        client: The ``httpx.AsyncClient`` to use.
        url: Target URL.
        error_class: Exception class to raise on exhausted retries.
        **kwargs: Extra keyword arguments forwarded to ``client.get()``.

    Returns:
        The successful HTTP response. 404 responses are returned
        as-is for the caller to handle.

    Raises:
        error_class: If all retries are exhausted.
    """
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.get(url, **kwargs)

            if response.status_code == 404:
                return response

            if response.status_code >= 500:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    "HTTP %d for %s, retrying in %.1fs (attempt %d/%d)",
                    response.status_code, url, backoff, attempt, MAX_RETRIES,
                )
                last_error = error_class(f"HTTP {response.status_code} from {url}")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(backoff)
                continue

            response.raise_for_status()
            return response

        except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.ConnectError) as exc:
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            logger.warning(
                "Connection error for %s: %s, retrying in %.1fs (attempt %d/%d)",
                url, exc, backoff, attempt, MAX_RETRIES,
            )
            last_error = exc
            if attempt < MAX_RETRIES:
                await asyncio.sleep(backoff)

    raise error_class(
        f"Request failed for {url} after {MAX_RETRIES} retries: {last_error}"
    )
