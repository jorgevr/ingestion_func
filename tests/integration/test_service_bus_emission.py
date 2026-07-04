"""End-to-end Service Bus emission integration test.

Mocks the OEDI Data Lake client, ServiceBusEmitter, and IdempotencyStore,
then triggers the pipeline to verify correct emit_cloudevent / emit_dead_letter calls.
"""

from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.idempotency_store import IdempotencyResult


def _env_vars() -> dict[str, str]:
    """Minimal environment variables for config loading."""
    return {
        "PVDAQ_SITE_IDS": "2,34",
        "PVDAQ_LOOKBACK_HOURS": "1",
        "PVDAQ_CRON_SCHEDULE": "0 */15 * * * *",
        "SERVICE_BUS_QUEUE_NAME": "raw-energy-events",
        "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
        "ServiceBusConnection__fullyQualifiedNamespace": "test.servicebus.windows.net",
        "IDEMPOTENCY_TABLE_NAME": "PvdaqIdempotency",
        "TableStorageConnection__tableServiceUri": "https://test.table.core.windows.net",
        "TENANT_ID": "research",
        "MAPPING_VERSION_PVDAQ": "unknown",
        "SCHEMA_VERSION_PVDAQ": "v1",
    }


def _mock_oedi_client(site_records: dict[int, list[dict]]) -> AsyncMock:
    """Create a mock OediDataLakeClient returning records keyed by site_id."""
    client = AsyncMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=None)

    async def _fetch(system_id: int, year: int, month: int, day: int) -> list[dict]:
        return site_records.get(system_id, [])

    client.fetch_daily_site_data = AsyncMock(side_effect=_fetch)
    return client


def _mock_emitter() -> AsyncMock:
    """Create a mock ServiceBusEmitter with async context manager support."""
    emitter = AsyncMock()
    emitter.__aenter__ = AsyncMock(return_value=emitter)
    emitter.__aexit__ = AsyncMock(return_value=None)
    return emitter


def _mock_idem_store() -> AsyncMock:
    """Create a mock IdempotencyStore that treats all records as new."""
    store = AsyncMock()
    store.__aenter__ = AsyncMock(return_value=store)
    store.__aexit__ = AsyncMock(return_value=None)
    store.check_and_reserve = AsyncMock(return_value=IdempotencyResult.NEW)
    store.mark_completed = AsyncMock(return_value=None)
    return store


class TestEndToEndEmission:
    """Pipeline integration: OEDI CSV -> validate -> idempotency -> enrich -> emit."""

    @pytest.mark.asyncio
    async def test_valid_records_emitted(self) -> None:
        """Valid records from 2 sites produce correct emit_cloudevent calls."""
        site_records = {
            2: [
                {"SiteID": 2, "measdatetime": "2026-01-15 12:00:00", "dc_power": 4800.0},
                {"SiteID": 2, "measdatetime": "2026-01-15 12:05:00", "dc_power": 4810.0},
            ],
            34: [
                {"SiteID": 34, "measdatetime": "2026-01-15 12:00:00", "dc_power": 3200.0},
            ],
        }

        oedi_client = _mock_oedi_client(site_records)
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        assert emitter.emit_cloudevent.call_count == 3
        assert emitter.emit_dead_letter.call_count == 0
        assert idem_store.check_and_reserve.call_count == 3
        assert idem_store.mark_completed_for.call_count == 3

    @pytest.mark.asyncio
    async def test_invalid_records_dead_lettered(self) -> None:
        """Invalid records are dead-lettered, valid ones are emitted."""
        site_records = {
            2: [
                {"SiteID": 2, "measdatetime": "2026-01-15 12:00:00", "dc_power": 4800.0},
                {"measdatetime": "2026-01-15 12:05:00", "dc_power": 4810.0},  # Missing SiteID
            ],
            34: [],
        }

        oedi_client = _mock_oedi_client(site_records)
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        assert emitter.emit_cloudevent.call_count == 1
        assert emitter.emit_dead_letter.call_count == 1

    @pytest.mark.asyncio
    async def test_empty_data_no_emission(self) -> None:
        """No emission when OEDI returns zero records for all sites."""
        oedi_client = _mock_oedi_client({2: [], 34: []})
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        assert emitter.emit_cloudevent.call_count == 0
        assert emitter.emit_dead_letter.call_count == 0

    @pytest.mark.asyncio
    async def test_site_discovery_from_oedi(self) -> None:
        """When PVDAQ_SITE_IDS is empty, sites are discovered from OEDI systems CSV."""
        site_records = {
            2: [{"SiteID": 2, "measdatetime": "2026-01-15 12:00:00", "dc_power": 4800.0}],
            34: [{"SiteID": 34, "measdatetime": "2026-01-15 12:00:00", "dc_power": 3200.0}],
        }

        oedi_client = _mock_oedi_client(site_records)
        oedi_client.fetch_systems_list = AsyncMock(return_value=[2, 34, 56])
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()

        env = _env_vars()
        env["PVDAQ_SITE_IDS"] = ""  # Empty — triggers discovery
        env["PVDAQ_SITE_COUNT"] = "2"

        with patch.dict(os.environ, env, clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        oedi_client.fetch_systems_list.assert_called_once()
        # Only first 2 sites used (pvdaq_site_count=2), so 2 records emitted
        assert emitter.emit_cloudevent.call_count == 2

    @pytest.mark.asyncio
    async def test_oedi_access_error_continues_to_next_site(self) -> None:
        """OediAccessError on one site doesn't prevent processing other sites."""
        from src.oedi_data_lake import OediAccessError

        oedi_client = AsyncMock()
        oedi_client.__aenter__ = AsyncMock(return_value=oedi_client)
        oedi_client.__aexit__ = AsyncMock(return_value=None)

        async def _fetch(system_id: int, year: int, month: int, day: int) -> list[dict]:
            if system_id == 2:
                raise OediAccessError("S3 timeout for site 2")
            return [{"SiteID": 34, "measdatetime": "2026-01-15 12:00:00", "dc_power": 3200.0}]

        oedi_client.fetch_daily_site_data = AsyncMock(side_effect=_fetch)
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        # Site 2 failed, site 34 succeeded — 1 record emitted
        assert emitter.emit_cloudevent.call_count == 1

    @pytest.mark.asyncio
    async def test_duplicate_records_skipped(self) -> None:
        """Duplicate records (per idempotency store) are skipped, not emitted."""
        site_records = {
            2: [
                {"SiteID": 2, "measdatetime": "2026-01-15 12:00:00", "dc_power": 4800.0},
                {"SiteID": 2, "measdatetime": "2026-01-15 12:05:00", "dc_power": 4810.0},
            ],
            34: [],
        }

        oedi_client = _mock_oedi_client(site_records)
        emitter = _mock_emitter()
        idem_store = _mock_idem_store()
        # First record is new, second is a duplicate
        idem_store.check_and_reserve = AsyncMock(
            side_effect=[IdempotencyResult.NEW, IdempotencyResult.DUPLICATE]
        )

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            await pvdaq_ingestion(timer)

        # Only 1 emitted (second was duplicate)
        assert emitter.emit_cloudevent.call_count == 1
        assert idem_store.mark_completed_for.call_count == 1

    @pytest.mark.asyncio
    async def test_generic_exception_in_record_processing(self) -> None:
        """Unexpected exception during record processing is caught and logged."""
        site_records = {
            2: [{"SiteID": 2, "measdatetime": "2026-01-15 12:00:00", "dc_power": 4800.0}],
            34: [],
        }

        oedi_client = _mock_oedi_client(site_records)
        emitter = _mock_emitter()
        emitter.emit_cloudevent = AsyncMock(side_effect=RuntimeError("unexpected boom"))
        idem_store = _mock_idem_store()

        with patch.dict(os.environ, _env_vars(), clear=True), \
             patch("function_app.OediDataLakeClient", return_value=oedi_client), \
             patch("function_app.ServiceBusEmitter", return_value=emitter), \
             patch("function_app.IdempotencyStore", return_value=idem_store):
            from function_app import pvdaq_ingestion

            timer = MagicMock()
            timer.past_due = False
            # Should NOT raise — exception is caught inside the pipeline
            await pvdaq_ingestion(timer)
