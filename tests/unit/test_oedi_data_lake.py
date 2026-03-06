"""Unit tests for the OEDI Data Lake client.

Tests cover: systems list fetch, daily CSV fetch with normalization,
sensor suffix stripping, 404 handling, retry on 5xx, and timeout errors.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from src.oedi_data_lake import OediAccessError, OediDataLakeClient, _normalize_record

BUCKET_URL = "https://oedi-data-lake.s3.amazonaws.com"
SYSTEMS_KEY = "pvdaq/csv/systems_20250729.csv"
DATA_PREFIX = "pvdaq/csv/pvdata"

SYSTEMS_CSV = (
    "system_id,system_name,state,lat,lon\n"
    "2,Test Site A,CO,39.74,-104.99\n"
    "34,Test Site B,CA,34.05,-118.24\n"
    "56,Test Site C,NY,40.71,-74.01\n"
)

DAILY_CSV = (
    "measured_on,system_id,dc_power__346,poa_irradiance__345,module_temp_1__349\n"
    "2026-01-15 12:00:00,2,4800.5,950.2,38.7\n"
    "2026-01-15 12:05:00,2,4810.0,951.0,38.9\n"
)


@pytest.fixture()
def client() -> OediDataLakeClient:
    return OediDataLakeClient(
        bucket_url=BUCKET_URL,
        systems_key=SYSTEMS_KEY,
        data_prefix=DATA_PREFIX,
    )


class TestFetchSystemsList:
    """Systems CSV is parsed into sorted list of integer IDs."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_sorted_system_ids(self, client: OediDataLakeClient) -> None:
        respx.get(f"{BUCKET_URL}/{SYSTEMS_KEY}").mock(
            return_value=httpx.Response(200, text=SYSTEMS_CSV)
        )

        result = await client.fetch_systems_list()

        assert result == [2, 34, 56]

    @respx.mock
    @pytest.mark.asyncio
    async def test_404_raises_error(self, client: OediDataLakeClient) -> None:
        respx.get(f"{BUCKET_URL}/{SYSTEMS_KEY}").mock(
            return_value=httpx.Response(404)
        )

        with pytest.raises(OediAccessError, match="not found"):
            await client.fetch_systems_list()


class TestFetchDailySiteData:
    """Daily CSV is fetched, parsed, and records normalized."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_normalized_records(self, client: OediDataLakeClient) -> None:
        url = f"{BUCKET_URL}/{DATA_PREFIX}/system_id=2/year=2026/month=1/day=15/system_2__date_2026_01_15.csv"
        respx.get(url).mock(return_value=httpx.Response(200, text=DAILY_CSV))

        records = await client.fetch_daily_site_data(system_id=2, year=2026, month=1, day=15)

        assert len(records) == 2
        assert records[0]["SiteID"] == 2
        assert records[0]["measdatetime"] == "2026-01-15 12:00:00"
        # Sensor suffixes stripped
        assert "dc_power" in records[0]
        assert "dc_power__346" not in records[0]
        assert records[0]["dc_power"] == 4800.5

    @respx.mock
    @pytest.mark.asyncio
    async def test_404_returns_empty_list(self, client: OediDataLakeClient) -> None:
        url = f"{BUCKET_URL}/{DATA_PREFIX}/system_id=2/year=2026/month=1/day=15/system_2__date_2026_01_15.csv"
        respx.get(url).mock(return_value=httpx.Response(404))

        records = await client.fetch_daily_site_data(system_id=2, year=2026, month=1, day=15)

        assert records == []


class TestRetryBehavior:
    """5xx and timeout errors trigger retries with backoff."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_retries_on_500(self, client: OediDataLakeClient) -> None:
        url = f"{BUCKET_URL}/{DATA_PREFIX}/system_id=2/year=2026/month=1/day=15/system_2__date_2026_01_15.csv"
        route = respx.get(url)
        route.side_effect = [
            httpx.Response(500),
            httpx.Response(200, text=DAILY_CSV),
        ]

        records = await client.fetch_daily_site_data(system_id=2, year=2026, month=1, day=15)

        assert len(records) == 2
        assert route.call_count == 2

    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_after_max_retries(self, client: OediDataLakeClient) -> None:
        url = f"{BUCKET_URL}/{DATA_PREFIX}/system_id=2/year=2026/month=1/day=15/system_2__date_2026_01_15.csv"
        route = respx.get(url)
        route.side_effect = [
            httpx.Response(500),
            httpx.Response(500),
            httpx.Response(500),
        ]

        with pytest.raises(OediAccessError, match="after 3 retries"):
            await client.fetch_daily_site_data(system_id=2, year=2026, month=1, day=15)

    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_on_timeout(self, client: OediDataLakeClient) -> None:
        url = f"{BUCKET_URL}/{DATA_PREFIX}/system_id=2/year=2026/month=1/day=15/system_2__date_2026_01_15.csv"
        route = respx.get(url)
        route.side_effect = [
            httpx.ConnectTimeout("Connection timed out"),
            httpx.ConnectTimeout("Connection timed out"),
            httpx.ConnectTimeout("Connection timed out"),
        ]

        with pytest.raises(OediAccessError, match="after 3 retries"):
            await client.fetch_daily_site_data(system_id=2, year=2026, month=1, day=15)


class TestNormalizeRecord:
    """Record normalization: field mapping, suffix stripping, numeric casting."""

    def test_maps_system_id_and_measured_on(self) -> None:
        row = {"system_id": "2", "measured_on": "2026-01-15 12:00:00", "dc_power__346": "4800.5"}
        result = _normalize_record(row)

        assert result is not None
        assert result["SiteID"] == 2
        assert result["measdatetime"] == "2026-01-15 12:00:00"

    def test_strips_sensor_suffixes(self) -> None:
        row = {
            "system_id": "2",
            "measured_on": "2026-01-15 12:00:00",
            "dc_power__346": "4800.5",
            "poa_irradiance__345": "950.2",
            "module_temp_1__349": "38.7",
        }
        result = _normalize_record(row)

        assert result is not None
        assert "dc_power" in result
        assert "poa_irradiance" in result
        assert "module_temp_1" in result
        assert "dc_power__346" not in result

    def test_casts_numeric_values(self) -> None:
        row = {"system_id": "2", "measured_on": "2026-01-15 12:00:00", "dc_power__346": "4800.5"}
        result = _normalize_record(row)

        assert result is not None
        assert isinstance(result["dc_power"], float)
        assert result["dc_power"] == 4800.5

    def test_missing_system_id_returns_none(self) -> None:
        row = {"measured_on": "2026-01-15 12:00:00", "dc_power__346": "4800.5"}
        result = _normalize_record(row)
        assert result is None

    def test_missing_measured_on_returns_none(self) -> None:
        row = {"system_id": "2", "dc_power__346": "4800.5"}
        result = _normalize_record(row)
        assert result is None

    def test_empty_values_skipped(self) -> None:
        row = {"system_id": "2", "measured_on": "2026-01-15 12:00:00", "dc_power__346": ""}
        result = _normalize_record(row)

        assert result is not None
        assert "dc_power" not in result

    def test_non_numeric_system_id_returns_none(self) -> None:
        """system_id that can't be cast to int returns None."""
        row = {"system_id": "abc", "measured_on": "2026-01-15 12:00:00", "dc_power__346": "4800.5"}
        result = _normalize_record(row)
        assert result is None

    def test_non_numeric_field_kept_as_string(self) -> None:
        """Non-numeric field values are kept as strings, not discarded."""
        row = {
            "system_id": "2",
            "measured_on": "2026-01-15 12:00:00",
            "status__999": "active",
        }
        result = _normalize_record(row)

        assert result is not None
        assert result["status"] == "active"


class TestFetchSystemsListEdgeCases:
    """Edge cases for systems list parsing."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_non_integer_system_ids_skipped(self, client: OediDataLakeClient) -> None:
        """Non-integer system_id rows are skipped with a warning."""
        csv_with_bad_id = (
            "system_id,system_name\n"
            "2,Good Site\n"
            "abc,Bad Site\n"
            "34,Another Good\n"
        )
        respx.get(f"{BUCKET_URL}/{SYSTEMS_KEY}").mock(
            return_value=httpx.Response(200, text=csv_with_bad_id)
        )

        result = await client.fetch_systems_list()
        assert result == [2, 34]


class TestAsyncContextManager:
    """OediDataLakeClient async context manager lifecycle."""

    @pytest.mark.asyncio
    async def test_context_manager_creates_and_closes_client(self) -> None:
        """__aenter__ returns self, __aexit__ closes the internal httpx client."""
        async with OediDataLakeClient(
            bucket_url=BUCKET_URL,
            systems_key=SYSTEMS_KEY,
            data_prefix=DATA_PREFIX,
        ) as client:
            assert isinstance(client, OediDataLakeClient)
        # After __aexit__, internal client is closed (no error)

    @pytest.mark.asyncio
    async def test_close_with_injected_client_does_not_close(self) -> None:
        """When an external httpx client is injected, close() does NOT close it."""
        import httpx as _httpx
        external = _httpx.AsyncClient()
        client = OediDataLakeClient(
            bucket_url=BUCKET_URL,
            systems_key=SYSTEMS_KEY,
            data_prefix=DATA_PREFIX,
            http_client=external,
        )
        await client.close()
        # external client is still open (not owned)
        assert not external.is_closed
        await external.aclose()

    @pytest.mark.asyncio
    async def test_close_owned_client(self) -> None:
        """When no httpx client is injected and _get_client was called, close() shuts it down."""
        client = OediDataLakeClient(
            bucket_url=BUCKET_URL,
            systems_key=SYSTEMS_KEY,
            data_prefix=DATA_PREFIX,
        )
        # Simulate that _get_client was called, creating an internal client
        internal = httpx.AsyncClient()
        client._http_client = internal

        await client.close()
        assert internal.is_closed
