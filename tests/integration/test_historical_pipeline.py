"""Integration tests for the historical ingestion pipeline (feature 002).

Tests cover:
- Dispatcher: lists files, filters unprocessed, enqueues work items with category
- Worker: streams S3 → ADLS, registers metadata, emits dataset CloudEvent
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import HistoricalConfig


def _historical_config() -> HistoricalConfig:
    return HistoricalConfig(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_historical_prefix="pvdaq/2023-solar-data-prize",
        pvdaq_historical_site_ids=[9068, 9069],
        pvdaq_historical_cron_schedule="0 0 */6 * * *",
        pvdaq_historical_queue_name="pvdaq-historical-work",
        service_bus_queue_name="raw-energy-events",
        dead_letter_queue_name="pvdaq-dead-letter",
        service_bus_fully_qualified_namespace="test-sb.servicebus.windows.net",
        file_tracking_table_name="PvdaqFileTracking",
        table_storage_uri="https://teststorage.table.core.windows.net",
        adls_account_url="https://testaccount.dfs.core.windows.net",
        adls_container_name="bronze",
        tenant_id="default",
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
        """Two sites with files: all unprocessed → all enqueued with category."""
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

        # Verify work item structure includes category
        first_call = mock_emitter.send_queue_message.call_args_list[0]
        work_item = first_call.kwargs["message_body"]
        assert work_item["site_id"] == 9068
        assert "s3_key" in work_item
        assert "file_name" in work_item
        assert "category" in work_item
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
        "category": "ac_power",
        "correlation_id": "test-corr-id",
        "enqueued_at": "2024-01-15T12:00:00Z",
        "last_modified": "2024-01-15T12:00:00Z",
    }


class TestWorkerIntegration:
    """Worker streams S3 → ADLS, registers metadata, emits dataset CloudEvent."""

    @pytest.mark.asyncio
    async def test_worker_streams_file_to_adls(self) -> None:
        """Valid file → stream_upload called, mark_completed with metadata, emit_cloudevent once."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(65000000, "abc123hash", 105121))
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_completed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_tracker.mark_processing.assert_called_once_with(9068, work_item["s3_key"], 1)
        # stream_upload called with source_url and adls path
        mock_adls.stream_upload.assert_called_once()
        call_kwargs = mock_adls.stream_upload.call_args
        assert "source_url" in call_kwargs.kwargs or len(call_kwargs.args) >= 1
        # metadata.json written alongside CSV
        mock_adls.write_json.assert_called_once()
        # One dataset CloudEvent emitted
        emitter.emit_cloudevent.assert_called_once()
        # mark_completed called with metadata
        mock_tracker.mark_completed.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_envelope_has_correct_type(self) -> None:
        """Emitted envelope uses solar.pvdaq.dataset.available type."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(65000000, "abc123hash", 105121))
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        emitter.emit_cloudevent.assert_called_once()
        envelope = emitter.emit_cloudevent.call_args.kwargs["envelope"]
        assert envelope["type"] == "solar.pvdaq.dataset.available"

    @pytest.mark.asyncio
    async def test_worker_envelope_data_block(self) -> None:
        """Dataset CloudEvent data block contains all required fields."""
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(65000000, "abc123hash", 105121))
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        envelope = emitter.emit_cloudevent.call_args.kwargs["envelope"]
        data = envelope["data"]
        assert data["site_id"] == 9068
        assert data["category"] == "ac_power"
        assert data["file_format"] == "csv"
        assert "storage_path" in data
        assert "ingestion_id" in data
        assert "source_url" in data
        assert data["file_size"] == 65000000
        assert data["file_hash"] == "abc123hash"

    @pytest.mark.asyncio
    async def test_worker_marks_failed_on_upload_error(self) -> None:
        """Worker marks file as failed and dead-letters when upload raises."""
        from src.adls_store import AdlsUploadError

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(
            side_effect=AdlsUploadError("Upload failed"),
        )
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_failed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            with pytest.raises(AdlsUploadError):
                await historical_worker(mock_msg)

        mock_tracker.mark_failed.assert_called_once_with(9068, work_item["s3_key"], 1)
        emitter.emit_dead_letter.assert_called_once()
        dead_letter_body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert dead_letter_body["file_reference"] == work_item["s3_key"]
        assert "failure_reason" in dead_letter_body
        assert dead_letter_body["correlation_id"] == "test-corr-id"

    @pytest.mark.asyncio
    async def test_worker_deterministic_adls_path(self) -> None:
        """ADLS path follows bronze convention: source=pvdaq/dataset={d}/ingestion_date={date}/{d}_v{n}.csv."""
        from datetime import datetime, timezone

        work_item = _default_work_item(site_id=9068, file_name="9068_ac_power_data.csv")
        mock_msg = _mock_work_item_msg(work_item)

        captured_paths: list[str] = []

        async def capture_upload(source_url: str, file_path: str) -> tuple[int, str, int]:
            captured_paths.append(file_path)
            return (1000, "deadbeef" * 8, 500)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = capture_upload
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])  # first ingestion → v1
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        expected_path = (
            f"source=pvdaq/dataset=9068_ac_power"
            f"/ingestion_date={today}"
            f"/9068_ac_power_v1.csv"
        )

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert len(captured_paths) == 1
        assert captured_paths[0] == expected_path

    @pytest.mark.asyncio
    async def test_worker_writes_metadata_json(self) -> None:
        """Worker writes a metadata.json sidecar conforming to contracts/metadata-file.json."""
        from datetime import datetime, timezone

        work_item = _default_work_item(site_id=9068, file_name="9068_ac_power_data.csv")
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(65000000, "a" * 64, 105121))
        captured_json_args: list[tuple] = []

        async def capture_write_json(file_path: str, data: dict) -> None:
            captured_json_args.append((file_path, data))

        mock_adls.write_json = capture_write_json
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        with (
            patch("function_app.load_historical_config", return_value=_historical_config()),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert len(captured_json_args) == 1
        meta_path, meta = captured_json_args[0]
        assert meta_path == (
            f"source=pvdaq/dataset=9068_ac_power/ingestion_date={today}/metadata.json"
        )
        # Validate 7-block structure
        assert meta["dataset"]["dataset_id"] == "9068_ac_power"
        assert meta["dataset"]["version"] == 1
        assert meta["dataset"]["schema_version"] == "unknown"
        assert meta["source"]["source"] == "pvdaq"
        assert meta["source"]["provider"] == "NREL"
        assert meta["ingestion"]["pipeline"] == "energy-ingestion-boundary-v1"
        assert meta["ingestion"]["trigger_type"] == "scheduled"
        assert meta["ingestion"]["file_size_bytes"] == 65000000
        assert meta["ingestion"]["checksum"] == "a" * 64
        assert meta["ingestion"]["status"] == "success"
        assert meta["ingestion"]["retry_count"] == 0
        assert meta["event_time"]["expected_frequency_seconds"] == 300
        assert meta["data_profile"]["row_count"] == 105121
        assert meta["quality_hint"]["notes"] == []
        assert meta["lineage"]["parent_dataset_version"] is None  # first ingestion


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
