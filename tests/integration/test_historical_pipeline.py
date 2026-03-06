"""Integration tests for the historical ingestion pipeline (feature 002).

Tests cover:
- Dispatcher: lists files, filters unprocessed, enqueues work items
- Worker: streams CSV, normalizes records, tracks file status
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import HistoricalConfig
from src.idempotency_store import IdempotencyResult


def _historical_config() -> HistoricalConfig:
    return HistoricalConfig(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_historical_prefix="pvdaq/2023-solar-data-prize",
        pvdaq_historical_site_ids=[9068, 9069],
        pvdaq_historical_cron_schedule="0 0 */6 * * *",
        pvdaq_historical_queue_name="pvdaq-historical-work",
        service_bus_topic_name="raw-energy-events",
        dead_letter_queue_name="pvdaq-dead-letter",
        service_bus_fully_qualified_namespace="test-sb.servicebus.windows.net",
        file_tracking_table_name="PvdaqFileTracking",
        table_storage_uri="https://teststorage.table.core.windows.net",
        idempotency_table_name="PvdaqIdempotency",
        tenant_id="research",
        mapping_version_pvdaq="unknown",
        schema_version_pvdaq="v1",
    )


_PREFIX = "pvdaq/2023-solar-data-prize"


def _file_info(site_id: int, name: str, size: int = 100) -> dict:
    return {
        "key": f"{_PREFIX}/{site_id}_OEDI/data/{name}",
        "size": size,
        "last_modified": "2024-01-15T12:00:00Z",
    }


class TestDispatcherIntegration:
    """Dispatcher lists files, filters via tracker, enqueues work items."""

    @pytest.mark.asyncio
    async def test_dispatcher_enqueues_unprocessed_files(self) -> None:
        """Two sites with files: all unprocessed → all enqueued."""
        site_9068_files = [
            _file_info(9068, "9068_ac_power_data.csv", 65000000),
            _file_info(9068, "9068_environment_data.csv", 288000000),
        ]
        site_9069_files = [
            _file_info(9069, "9069_ac_power_data.csv", 50000000),
        ]

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(side_effect=[site_9068_files, site_9069_files])
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        # All files are unprocessed
        mock_tracker.get_unprocessed_files = AsyncMock(side_effect=[site_9068_files, site_9069_files])
        mock_tracker.mark_queued = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.send_queue_message = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=mock_emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_dispatcher

            timer = MagicMock()
            await historical_dispatcher(timer)

        # 3 total files enqueued
        assert mock_emitter.send_queue_message.call_count == 3
        assert mock_tracker.mark_queued.call_count == 3

        # Verify work item structure
        first_call = mock_emitter.send_queue_message.call_args_list[0]
        work_item = first_call.kwargs["message_body"]
        assert work_item["site_id"] == 9068
        assert "s3_key" in work_item
        assert "file_name" in work_item
        assert "correlation_id" in work_item
        assert "enqueued_at" in work_item

    @pytest.mark.asyncio
    async def test_dispatcher_skips_already_processed(self) -> None:
        """All files already processed → zero enqueued."""
        files = [_file_info(9068, "9068_ac_power_data.csv")]

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(side_effect=[files, []])
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_unprocessed_files = AsyncMock(side_effect=[[], []])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=mock_emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_dispatcher

            timer = MagicMock()
            await historical_dispatcher(timer)

        mock_emitter.send_queue_message.assert_not_called()


def _mock_idem_store(result: IdempotencyResult = IdempotencyResult.NEW) -> AsyncMock:
    """Create a mock IdempotencyStore that returns the given result."""
    store = AsyncMock()
    store.check_and_reserve_historical = AsyncMock(return_value=result)
    store.mark_completed = AsyncMock()
    store.__aenter__ = AsyncMock(return_value=store)
    store.__aexit__ = AsyncMock(return_value=False)
    return store


def _mock_emitter() -> AsyncMock:
    """Create a mock ServiceBusEmitter with context manager support."""
    emitter = AsyncMock()
    emitter.emit_cloudevent = AsyncMock()
    emitter.emit_dead_letter = AsyncMock()
    emitter.__aenter__ = AsyncMock(return_value=emitter)
    emitter.__aexit__ = AsyncMock(return_value=False)
    return emitter


def _mock_work_item_msg(work_item: dict) -> MagicMock:
    mock_msg = MagicMock()
    mock_msg.get_body.return_value = json.dumps(work_item).encode("utf-8")
    return mock_msg


def _default_work_item(site_id: int = 9068, file_name: str = "9068_ac_power_data.csv") -> dict:
    return {
        "site_id": site_id,
        "s3_key": f"{_PREFIX}/{site_id}_OEDI/data/{file_name}",
        "file_name": file_name,
        "correlation_id": "test-corr-id",
        "enqueued_at": "2024-01-15T12:00:00Z",
    }


class TestWorkerIntegration:
    """Worker streams CSV, normalizes, validates, and emits."""

    @pytest.mark.asyncio
    async def test_worker_processes_valid_csv_file(self) -> None:
        """Valid rows → emit_cloudevent called, mark_completed with count."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        csv_rows = [
            {"measured_on": "2023-01-01 00:00:00", "dc_power_123": "100.5", "temp_456": "25.0"},
            {"measured_on": "2023-01-01 00:05:00", "dc_power_123": "101.0", "temp_456": "25.1"},
        ]

        async def mock_stream(s3_key: str):
            for row in csv_rows:
                yield row

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_completed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        idem = _mock_idem_store(IdempotencyResult.NEW)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_tracker.mark_processing.assert_called_once_with(9068, work_item["s3_key"])
        # 2 valid records emitted
        assert emitter.emit_cloudevent.call_count == 2
        assert idem.check_and_reserve_historical.call_count == 2
        mock_tracker.mark_completed.assert_called_once_with(9068, work_item["s3_key"], 2)

    @pytest.mark.asyncio
    async def test_worker_dead_letters_invalid_records(self) -> None:
        """Invalid records → emit_dead_letter called, valid ones emitted."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        csv_rows = [
            {"measured_on": "2023-01-01 00:00:00", "dc_power_123": "100.5"},
            {"measured_on": "2023-01-01 00:05:00", "dc_power_123": "bad_value"},
        ]

        async def mock_stream(s3_key: str):
            for row in csv_rows:
                yield row

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        idem = _mock_idem_store(IdempotencyResult.NEW)

        call_count = 0

        def mock_validate(record: dict) -> tuple[bool, list[dict]]:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return True, []
            return False, [{"message": "test error", "path": "$", "validator": "type"}]

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.validate_record", side_effect=mock_validate),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert emitter.emit_cloudevent.call_count == 1
        assert emitter.emit_dead_letter.call_count == 1
        mock_tracker.mark_completed.assert_called_once_with(9068, work_item["s3_key"], 1)

    @pytest.mark.asyncio
    async def test_worker_envelope_has_correct_type_and_source(self) -> None:
        """Emitted envelopes use historical event type and source."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        csv_rows = [
            {"measured_on": "2023-06-15T12:00:00", "ac_power_100": "4500.0"},
        ]

        async def mock_stream(s3_key: str):
            for row in csv_rows:
                yield row

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        idem = _mock_idem_store(IdempotencyResult.NEW)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        emitter.emit_cloudevent.assert_called_once()
        envelope = emitter.emit_cloudevent.call_args.kwargs["envelope"]
        assert envelope["type"] == "raw.pvdaq.historical.v1"
        assert envelope["source"] == "/energy-ingestion-boundary/pvdaq-historical"

    @pytest.mark.asyncio
    async def test_worker_marks_failed_on_error(self) -> None:
        """Worker marks file as failed when streaming raises."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        async def mock_stream_error(s3_key: str):
            raise RuntimeError("Network error")
            yield  # noqa: RET503 — make it an async generator

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream_error
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_failed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        idem = _mock_idem_store()

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            with pytest.raises(RuntimeError, match="Network error"):
                await historical_worker(mock_msg)

        mock_tracker.mark_failed.assert_called_once_with(9068, work_item["s3_key"])

    @pytest.mark.asyncio
    async def test_worker_skips_duplicates(self) -> None:
        """Duplicate records are skipped, not emitted."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        csv_rows = [
            {"measured_on": "2023-01-01 00:00:00", "dc_power_123": "100.5"},
            {"measured_on": "2023-01-01 00:05:00", "dc_power_123": "101.0"},
        ]

        async def mock_stream(s3_key: str):
            for row in csv_rows:
                yield row

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        # All records are duplicates
        idem = _mock_idem_store(IdempotencyResult.DUPLICATE)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        emitter.emit_cloudevent.assert_not_called()
        # 0 emitted, mark_completed with 0
        mock_tracker.mark_completed.assert_called_once_with(9068, work_item["s3_key"], 0)

    @pytest.mark.asyncio
    async def test_worker_retries_pending_records(self) -> None:
        """RETRY_EMIT records are re-emitted (crash recovery)."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        csv_rows = [
            {"measured_on": "2023-01-01 00:00:00", "dc_power_123": "100.5"},
        ]

        async def mock_stream(s3_key: str):
            for row in csv_rows:
                yield row

        mock_oedi = AsyncMock()
        mock_oedi.stream_csv_rows = mock_stream
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        idem = _mock_idem_store(IdempotencyResult.RETRY_EMIT)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.IdempotencyStore", return_value=idem),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        # RETRY_EMIT → record is emitted
        emitter.emit_cloudevent.assert_called_once()


class TestIncrementalDetection:
    """Incremental file detection: only new/changed files enqueued."""

    @pytest.mark.asyncio
    async def test_new_file_added_enqueued_only(self) -> None:
        """Second run with one new file → only the new file enqueued."""
        existing_file = _file_info(9068, "9068_ac_power_data.csv", 65000000)
        new_file = _file_info(9068, "9068_tracker_data.csv", 870000000)

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(side_effect=[[existing_file, new_file], []])
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        # Only the new file is unprocessed
        mock_tracker.get_unprocessed_files = AsyncMock(side_effect=[[new_file], []])
        mock_tracker.mark_queued = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.send_queue_message = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=mock_emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_dispatcher

            timer = MagicMock()
            await historical_dispatcher(timer)

        assert mock_emitter.send_queue_message.call_count == 1
        work_item = mock_emitter.send_queue_message.call_args.kwargs["message_body"]
        assert "9068_tracker_data.csv" in work_item["file_name"]

    @pytest.mark.asyncio
    async def test_dispatcher_continues_on_site_error(self) -> None:
        """If one site fails, other sites are still processed."""
        from src.oedi_historical_client import OediHistoricalAccessError

        site_9069_files = [_file_info(9069, "9069_ac_power_data.csv")]

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(
            side_effect=[OediHistoricalAccessError("S3 error"), site_9069_files],
        )
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_unprocessed_files = AsyncMock(return_value=site_9069_files)
        mock_tracker.mark_queued = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.send_queue_message = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=mock_emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_dispatcher

            timer = MagicMock()
            await historical_dispatcher(timer)

        # Site 9068 failed, but site 9069 was still processed
        assert mock_emitter.send_queue_message.call_count == 1
