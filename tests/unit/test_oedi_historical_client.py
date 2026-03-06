"""Unit tests for src.oedi_historical_client — S3 listing + streaming CSV."""

from __future__ import annotations

import httpx
import pytest
import respx

from src.oedi_historical_client import OediHistoricalAccessError, OediHistoricalClient

BUCKET_URL = "https://oedi-data-lake.s3.amazonaws.com"
PREFIX = "pvdaq/2023-solar-data-prize"

# S3 XML namespace
_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _s3_xml_response(contents: list[tuple[str, int, str]], is_truncated: bool = False, next_token: str = "") -> str:
    """Build a mock S3 ListObjectsV2 XML response."""
    items = ""
    for key, size, last_modified in contents:
        items += f"""
    <Contents>
      <Key>{key}</Key>
      <Size>{size}</Size>
      <LastModified>{last_modified}</LastModified>
    </Contents>"""

    truncated = "true" if is_truncated else "false"
    token_el = f"<NextContinuationToken>{next_token}</NextContinuationToken>" if next_token else ""

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="{_NS}">
  <IsTruncated>{truncated}</IsTruncated>
  {token_el}
  {items}
</ListBucketResult>"""


@pytest.fixture()
def client() -> OediHistoricalClient:
    return OediHistoricalClient(
        bucket_url=BUCKET_URL,
        historical_prefix=PREFIX,
    )


class TestListCsvFiles:
    """list_csv_files parses S3 XML and returns CSV file metadata."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_single_page_listing(self, client: OediHistoricalClient) -> None:
        files = [
            (f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv", 65000000, "2024-01-15T12:00:00Z"),
            (f"{PREFIX}/9068_OEDI/data/9068_environment_data.csv", 288000000, "2024-01-15T12:00:00Z"),
        ]
        xml = _s3_xml_response(files)
        respx.get(BUCKET_URL).mock(return_value=httpx.Response(200, text=xml))

        result = await client.list_csv_files(9068)

        assert len(result) == 2
        assert result[0]["key"] == f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv"
        assert result[0]["size"] == 65000000
        assert result[1]["key"] == f"{PREFIX}/9068_OEDI/data/9068_environment_data.csv"

    @respx.mock
    @pytest.mark.asyncio
    async def test_paginated_listing(self, client: OediHistoricalClient) -> None:
        page1_files = [
            (f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv", 65000000, "2024-01-15T12:00:00Z"),
        ]
        page2_files = [
            (f"{PREFIX}/9068_OEDI/data/9068_environment_data.csv", 288000000, "2024-01-15T12:00:00Z"),
        ]
        xml1 = _s3_xml_response(page1_files, is_truncated=True, next_token="token123")
        xml2 = _s3_xml_response(page2_files)

        respx.get(BUCKET_URL).side_effect = [
            httpx.Response(200, text=xml1),
            httpx.Response(200, text=xml2),
        ]

        result = await client.list_csv_files(9068)
        assert len(result) == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_404_returns_empty_list(self, client: OediHistoricalClient) -> None:
        respx.get(BUCKET_URL).mock(return_value=httpx.Response(404))
        result = await client.list_csv_files(9999)
        assert result == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_non_csv_files_filtered_out(self, client: OediHistoricalClient) -> None:
        files = [
            (f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv", 100, "2024-01-15T12:00:00Z"),
            (f"{PREFIX}/9068_OEDI/data/readme.txt", 50, "2024-01-15T12:00:00Z"),
        ]
        xml = _s3_xml_response(files)
        respx.get(BUCKET_URL).mock(return_value=httpx.Response(200, text=xml))

        result = await client.list_csv_files(9068)
        assert len(result) == 1
        assert result[0]["key"].endswith(".csv")

    @respx.mock
    @pytest.mark.asyncio
    async def test_5xx_retry_then_success(self, client: OediHistoricalClient) -> None:
        files = [(f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv", 100, "2024-01-15T12:00:00Z")]
        xml = _s3_xml_response(files)

        respx.get(BUCKET_URL).side_effect = [
            httpx.Response(500),
            httpx.Response(200, text=xml),
        ]

        result = await client.list_csv_files(9068)
        assert len(result) == 1

    @respx.mock
    @pytest.mark.asyncio
    async def test_5xx_exhausted_raises(self, client: OediHistoricalClient) -> None:
        respx.get(BUCKET_URL).side_effect = [
            httpx.Response(500),
            httpx.Response(500),
            httpx.Response(500),
        ]

        with pytest.raises(OediHistoricalAccessError):
            await client.list_csv_files(9068)

    @respx.mock
    @pytest.mark.asyncio
    async def test_empty_listing(self, client: OediHistoricalClient) -> None:
        xml = _s3_xml_response([])
        respx.get(BUCKET_URL).mock(return_value=httpx.Response(200, text=xml))

        result = await client.list_csv_files(9068)
        assert result == []


class TestStreamCsvRows:
    """stream_csv_rows yields dicts from streamed CSV content."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_yields_rows_as_dicts(self, client: OediHistoricalClient) -> None:
        csv_content = "measured_on,dc_power_123,temp\n2023-01-01 00:00:00,100.5,25.0\n2023-01-01 00:05:00,101.0,25.1\n"
        s3_key = f"{PREFIX}/9068_OEDI/data/9068_ac_power_data.csv"
        url = f"{BUCKET_URL}/{s3_key}"

        respx.get(url).mock(return_value=httpx.Response(200, content=csv_content.encode()))

        rows = []
        async for row in client.stream_csv_rows(s3_key):
            rows.append(row)

        assert len(rows) == 2
        assert rows[0]["measured_on"] == "2023-01-01 00:00:00"
        assert rows[0]["dc_power_123"] == "100.5"
        assert rows[1]["measured_on"] == "2023-01-01 00:05:00"

    @respx.mock
    @pytest.mark.asyncio
    async def test_404_yields_nothing(self, client: OediHistoricalClient) -> None:
        s3_key = f"{PREFIX}/9068_OEDI/data/nonexistent.csv"
        url = f"{BUCKET_URL}/{s3_key}"

        respx.get(url).mock(return_value=httpx.Response(404))

        rows = []
        async for row in client.stream_csv_rows(s3_key):
            rows.append(row)
        assert rows == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_header_only_csv_yields_nothing(self, client: OediHistoricalClient) -> None:
        csv_content = "measured_on,dc_power\n"
        s3_key = f"{PREFIX}/9068_OEDI/data/9068_empty.csv"
        url = f"{BUCKET_URL}/{s3_key}"

        respx.get(url).mock(return_value=httpx.Response(200, content=csv_content.encode()))

        rows = []
        async for row in client.stream_csv_rows(s3_key):
            rows.append(row)
        assert rows == []


class TestOediHistoricalClientLifecycle:
    """Async context manager lifecycle."""

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        async with OediHistoricalClient(bucket_url=BUCKET_URL) as client:
            assert client is not None

    @pytest.mark.asyncio
    async def test_injected_client_not_closed(self) -> None:
        http_client = httpx.AsyncClient()
        client = OediHistoricalClient(bucket_url=BUCKET_URL, http_client=http_client)
        await client.close()
        assert not http_client.is_closed
        await http_client.aclose()
