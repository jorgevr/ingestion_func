"""Integration tests for OEDI Data Lake client.

Tests cover: multi-site sequential polling, multi-date iteration,
and partial site failure (one site 500, others succeed).
"""

from __future__ import annotations

import httpx
import pytest
import respx

from src.oedi_data_lake import OediAccessError, OediDataLakeClient

BUCKET_URL = "https://oedi-data-lake.s3.amazonaws.com"
SYSTEMS_KEY = "pvdaq/csv/systems_20250729.csv"
DATA_PREFIX = "pvdaq/csv/pvdata"


def _daily_csv(system_id: int, date_str: str, count: int = 2) -> str:
    """Build a mock daily CSV string with *count* records."""
    header = "measured_on,system_id,dc_power__346,poa_irradiance__345\n"
    rows = "\n".join(
        f"{date_str} 12:{i * 5:02d}:00,{system_id},{4800.0 + i},{950.0 + i}"
        for i in range(count)
    )
    return header + rows + "\n"


def _csv_url(system_id: int, year: int, month: int, day: int) -> str:
    filename = f"system_{system_id}__date_{year}_{month:02d}_{day:02d}.csv"
    return f"{BUCKET_URL}/{DATA_PREFIX}/system_id={system_id}/year={year}/month={month}/day={day}/{filename}"


@pytest.fixture()
def client() -> OediDataLakeClient:
    return OediDataLakeClient(
        bucket_url=BUCKET_URL,
        systems_key=SYSTEMS_KEY,
        data_prefix=DATA_PREFIX,
    )


class TestMultiSitePolling:
    """Multi-site sequential polling retrieves data from all sites."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_polls_multiple_sites(self, client: OediDataLakeClient) -> None:
        respx.get(_csv_url(2, 2026, 1, 15)).mock(
            return_value=httpx.Response(200, text=_daily_csv(2, "2026-01-15", count=3))
        )
        respx.get(_csv_url(34, 2026, 1, 15)).mock(
            return_value=httpx.Response(200, text=_daily_csv(34, "2026-01-15", count=2))
        )

        all_records: list[dict] = []
        for site_id in [2, 34]:
            records = await client.fetch_daily_site_data(
                system_id=site_id, year=2026, month=1, day=15,
            )
            all_records.extend(records)

        assert len(all_records) == 5
        assert all_records[0]["SiteID"] == 2
        assert all_records[3]["SiteID"] == 34


class TestMultiDateIteration:
    """Multiple dates for a single site."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_fetches_multiple_dates(self, client: OediDataLakeClient) -> None:
        respx.get(_csv_url(2, 2026, 1, 14)).mock(
            return_value=httpx.Response(200, text=_daily_csv(2, "2026-01-14", count=2))
        )
        respx.get(_csv_url(2, 2026, 1, 15)).mock(
            return_value=httpx.Response(200, text=_daily_csv(2, "2026-01-15", count=3))
        )

        all_records: list[dict] = []
        for year, month, day in [(2026, 1, 14), (2026, 1, 15)]:
            records = await client.fetch_daily_site_data(
                system_id=2, year=year, month=month, day=day,
            )
            all_records.extend(records)

        assert len(all_records) == 5


class TestPartialSiteFailure:
    """One site failing does not prevent retrieval from other sites."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_partial_failure_continues(self, client: OediDataLakeClient) -> None:
        """Site 2 returns 500 (all retries fail), site 34 succeeds."""
        route_2 = respx.get(_csv_url(2, 2026, 1, 15))
        route_2.side_effect = [
            httpx.Response(500),
            httpx.Response(500),
            httpx.Response(500),
        ]
        respx.get(_csv_url(34, 2026, 1, 15)).mock(
            return_value=httpx.Response(200, text=_daily_csv(34, "2026-01-15", count=2))
        )

        all_records: list[dict] = []
        failed_sites: list[int] = []

        for site_id in [2, 34]:
            try:
                records = await client.fetch_daily_site_data(
                    system_id=site_id, year=2026, month=1, day=15,
                )
                all_records.extend(records)
            except OediAccessError:
                failed_sites.append(site_id)

        assert len(all_records) == 2
        assert all_records[0]["SiteID"] == 34
        assert failed_sites == [2]
