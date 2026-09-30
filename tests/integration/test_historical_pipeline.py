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
        data_storage_account_url="https://testaccount.blob.core.windows.net",
        bronze_container="bronze",
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
        mock_oedi.list_csv_files = AsyncMock(
            side_effect=[site_9068_files, site_9069_files]
        )
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        # All files are unprocessed
        mock_tracker.get_unprocessed_files = AsyncMock(
            side_effect=[site_9068_files, site_9069_files]
        )
        mock_tracker.mark_queued = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.send_queue_message = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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


def _default_work_item(
    site_id: int = 9068, file_name: str = "9068_ac_power_data.csv"
) -> dict:
    return {
        "site_id": site_id,
        "s3_key": f"{_PREFIX}/{site_id}_OEDI/data/{file_name}",
        "file_name": file_name,
        "category": "ac_power",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
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
        mock_adls.stream_upload = AsyncMock(
            return_value=(65000000, "abc123hash", 105121)
        )
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_tracker.mark_processing.assert_called_once_with(
            9068, work_item["s3_key"], 1
        )
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
        mock_adls.stream_upload = AsyncMock(
            return_value=(65000000, "abc123hash", 105121)
        )
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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
        mock_adls.stream_upload = AsyncMock(
            return_value=(65000000, "abc123hash", 105121)
        )
        mock_adls.write_json = AsyncMock()
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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
    async def test_worker_marks_failed_and_dead_letters_without_raising_on_deterministic_upload_error(
        self,
    ) -> None:
        """A deterministic upload failure (unclassified — not recognised as
        network/5xx/throttling) marks the file failed, dead-letters, and
        completes the message rather than raising (AGENTS.md §6: only
        transient errors may redeliver — see also
        test_worker_reraises_on_transient_upload_error below)."""
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        mock_tracker.mark_failed.assert_called_once_with(9068, work_item["s3_key"], 1)
        emitter.emit_dead_letter.assert_called_once()
        dead_letter_body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert dead_letter_body["file_reference"] == work_item["s3_key"]
        assert "failure_reason" in dead_letter_body
        assert (
            dead_letter_body["correlation_id"] == "550e8400-e29b-41d4-a716-446655440000"
        )

    @pytest.mark.asyncio
    async def test_worker_dead_letters_without_raising_on_missing_source_file(
        self,
    ) -> None:
        """A 404 ("Source not found") is explicitly deterministic — retrying
        the identical URL can never succeed."""
        from src.adls_store import AdlsUploadError

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(
            side_effect=AdlsUploadError("Source not found: https://example.com/x.csv"),
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_reraises_on_transient_upload_error(self) -> None:
        """A 503 from the Blob SDK (wrapped as AdlsUploadError's __cause__)
        is transient — the worker must still dead-letter (for visibility)
        but also re-raise so Service Bus redelivers the message."""
        from azure.core.exceptions import HttpResponseError

        from src.adls_store import AdlsUploadError

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        transient_cause = HttpResponseError(message="Service unavailable")
        transient_cause.status_code = 503
        upload_error = AdlsUploadError("Failed to upload: 503")
        upload_error.__cause__ = transient_cause

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(side_effect=upload_error)
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            with pytest.raises(AdlsUploadError):
                await historical_worker(mock_msg)

        emitter.emit_dead_letter.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_invalid_work_item_dead_lettered_not_raised(self) -> None:
        """A work item that fails schema validation (e.g. missing a
        required field) dead-letters and completes the message — it must
        never raise into the trigger."""
        invalid_work_item = {
            "site_id": 9068,
            # missing s3_key, file_name, correlation_id, enqueued_at
        }
        mock_msg = _mock_work_item_msg(invalid_work_item)

        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()
        dead_letter_body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert dead_letter_body["error_type"] == "validation_failure"

    @pytest.mark.asyncio
    async def test_worker_deterministic_adls_path(self) -> None:
        """ADLS path follows bronze convention: source=pvdaq/dataset={d}/ingestion_date={date}/{d}_v{n}.csv."""
        from datetime import datetime, timezone

        work_item = _default_work_item(site_id=9068, file_name="9068_ac_power_data.csv")
        mock_msg = _mock_work_item_msg(work_item)

        captured_paths: list[str] = []

        async def capture_upload(
            source_url: str, file_path: str, s3_key: str
        ) -> tuple[int, str, int]:
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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

        async def capture_write_json(file_path: str, data: dict, s3_key: str) -> None:
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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

    @pytest.mark.asyncio
    async def test_worker_passes_the_real_s3_key_to_write_json_not_file_name(
        self,
    ) -> None:
        """F2: write_json's s3_key kwarg must be the work item's s3_key
        (the full S3 object key) — not file_name (just the basename).
        They're deliberately different strings in this fixture
        (_default_work_item's s3_key is f"{prefix}/{site_id}_OEDI/data/
        {file_name}") so a mix-up is caught here rather than passing
        coincidentally."""
        work_item = _default_work_item(site_id=9068, file_name="9068_ac_power_data.csv")
        assert work_item["s3_key"] != work_item["file_name"]
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(65000000, "a" * 64, 105121))
        captured_write_json_kwargs: list[dict] = []

        async def capture_write_json(file_path: str, data: dict, s3_key: str) -> None:
            captured_write_json_kwargs.append(
                {"file_path": file_path, "s3_key": s3_key}
            )

        mock_adls.write_json = capture_write_json
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert len(captured_write_json_kwargs) == 1
        assert captured_write_json_kwargs[0]["s3_key"] == work_item["s3_key"]
        assert captured_write_json_kwargs[0]["s3_key"] != work_item["file_name"]


class TestIncrementalDetection:
    """Incremental file detection: only new/changed files enqueued."""

    @pytest.mark.asyncio
    async def test_new_file_added_enqueued_only(self) -> None:
        """Second run with one new file → only the new file enqueued."""
        existing_file = _file_info(9068, "9068_ac_power_data.csv", 65000000)
        new_file = _file_info(9068, "9068_tracker_data.csv", 870000000)

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(
            side_effect=[[existing_file, new_file], []]
        )
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
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

    @pytest.mark.asyncio
    async def test_file_without_last_modified_skipped_and_logged(self) -> None:
        """No default: an S3 listing entry with no last_modified is skipped
        and logged, never enqueued (it would otherwise fail work-item schema
        validation's minLength:1 on the worker side anyway)."""
        good_file = _file_info(9068, "9068_ac_power_data.csv")
        bad_file = {
            "key": f"{_PREFIX}/9068_OEDI/data/9068_tracker_data.csv",
            "size": 100,
            # no last_modified key at all
        }

        mock_oedi = AsyncMock()
        mock_oedi.list_csv_files = AsyncMock(side_effect=[[good_file, bad_file], []])
        mock_oedi.__aenter__ = AsyncMock(return_value=mock_oedi)
        mock_oedi.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_unprocessed_files = AsyncMock(
            side_effect=[[good_file, bad_file], []]
        )
        mock_tracker.mark_queued = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        mock_emitter = AsyncMock()
        mock_emitter.send_queue_message = AsyncMock()
        mock_emitter.__aenter__ = AsyncMock(return_value=mock_emitter)
        mock_emitter.__aexit__ = AsyncMock(return_value=False)

        mock_logger = MagicMock()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.OediHistoricalClient", return_value=mock_oedi),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=mock_emitter),
            patch("function_app.create_logger", return_value=mock_logger),
        ):
            from function_app import historical_dispatcher

            timer = MagicMock()
            await historical_dispatcher(timer)

        # Only the good file was enqueued/tracked.
        assert mock_emitter.send_queue_message.call_count == 1
        assert mock_tracker.mark_queued.call_count == 1
        enqueued = mock_emitter.send_queue_message.call_args.kwargs["message_body"]
        assert enqueued["file_name"] == "9068_ac_power_data.csv"

        # The skip was logged as a warning naming the skipped file (site
        # 9069 also logs its own unrelated "no files found" warning).
        skip_calls = [
            call
            for call in mock_logger.warning.call_args_list
            if "9068_tracker_data.csv" in " ".join(str(a) for a in call.args)
        ]
        assert len(skip_calls) == 1


class TestWorkerMalformedBody:
    """The message body is decoded/parsed inside the guarded path — a
    malformed body is deterministic (retrying the identical bytes can never
    succeed): dead-lettered on the first delivery, never raised."""

    @pytest.mark.asyncio
    async def test_non_utf8_body_dead_lettered_not_raised(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.return_value = b"\xff\xfe\x00\x01 not valid utf-8"

        emitter = _mock_emitter()
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()
        body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert body["error_type"] == "malformed_body"

    @pytest.mark.asyncio
    async def test_non_json_body_dead_lettered_not_raised(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.return_value = b"this is not json {"

        emitter = _mock_emitter()
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()
        body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert body["error_type"] == "malformed_body"

    @pytest.mark.asyncio
    async def test_json_array_body_dead_lettered_not_raised(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.return_value = json.dumps([1, 2, 3]).encode("utf-8")

        emitter = _mock_emitter()
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()
        body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert body["error_type"] == "malformed_body"

    @pytest.mark.asyncio
    async def test_json_scalar_body_dead_lettered_not_raised(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.return_value = json.dumps("just a string").encode("utf-8")

        emitter = _mock_emitter()
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        emitter.emit_dead_letter.assert_called_once()
        body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert body["error_type"] == "malformed_body"


class TestMetricsEmittedOnEveryExitPath:
    """emit_dataset_metrics(stats) must run on every exit path — both
    deterministic dead-letter returns, the transient raise, and success —
    and stats.datasets_failed must be 1 on every failing path, including
    the two that return before ever building the "old" stats object (body
    parse failure and schema validation failure)."""

    @pytest.mark.asyncio
    async def test_malformed_body_emits_metrics_with_failed_flag(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.return_value = b"not json"
        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics") as mock_metrics,
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_metrics.assert_called_once()
        assert mock_metrics.call_args.args[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_schema_validation_failure_emits_metrics_with_failed_flag(
        self,
    ) -> None:
        invalid_work_item = {"site_id": 9068}
        mock_msg = _mock_work_item_msg(invalid_work_item)
        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics") as mock_metrics,
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_metrics.assert_called_once()
        assert mock_metrics.call_args.args[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_deterministic_processing_failure_emits_metrics(self) -> None:
        from src.adls_store import AdlsUploadError

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(side_effect=AdlsUploadError("boom"))
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics") as mock_metrics,
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)  # must NOT raise

        mock_metrics.assert_called_once()
        assert mock_metrics.call_args.args[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_transient_processing_failure_emits_metrics_before_raising(
        self,
    ) -> None:
        from azure.core.exceptions import HttpResponseError

        from src.adls_store import AdlsUploadError

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        transient_cause = HttpResponseError(message="unavailable")
        transient_cause.status_code = 503
        upload_error = AdlsUploadError("failed")
        upload_error.__cause__ = transient_cause

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(side_effect=upload_error)
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
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics") as mock_metrics,
        ):
            from function_app import historical_worker

            with pytest.raises(AdlsUploadError):
                await historical_worker(mock_msg)

        # Metrics still ran, via `finally`, even though the function raised.
        mock_metrics.assert_called_once()
        assert mock_metrics.call_args.args[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_success_emits_metrics_without_failed_flag(self) -> None:
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(100, "a" * 64, 10))
        mock_adls.write_json = AsyncMock()
        mock_adls.blob_url = AsyncMock(return_value="http://azurite/bronze/x.csv")
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics") as mock_metrics,
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        mock_metrics.assert_called_once()
        stats = mock_metrics.call_args.args[0]
        assert stats.datasets_failed == 0
        assert stats.datasets_emitted == 1


class TestCompletedMeansEventPublished:
    """Order: upload -> write_json -> emit_cloudevent -> mark_completed.
    mark_completed is the signal get_versions() reads to decide "already
    done", so it must not fire until after the event a consumer reacts to
    has actually been sent."""

    @pytest.mark.asyncio
    async def test_emit_cloudevent_happens_before_mark_completed(self) -> None:
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        call_order: list[str] = []

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(100, "a" * 64, 10))
        mock_adls.write_json = AsyncMock()
        mock_adls.blob_url = AsyncMock(return_value="http://azurite/bronze/x.csv")
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])

        async def _mark_completed(*args, **kwargs) -> None:
            call_order.append("mark_completed")

        mock_tracker.mark_completed = _mark_completed
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        async def _emit_cloudevent(*args, **kwargs) -> None:
            call_order.append("emit_cloudevent")

        emitter.emit_cloudevent = _emit_cloudevent

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert call_order == ["emit_cloudevent", "mark_completed"]


class TestRedeliveryAfterTransientEmitFailure:
    """A transient failure during emit_cloudevent must not have run
    mark_completed, so a redelivery of the same message resolves the same
    version (get_versions still sees no completed version), re-uploads
    (idempotent overwrite of the same path), and re-emits with the same
    deterministic id — the event is "emitted once" from a dedupe-by-id
    consumer's perspective, even though emit_cloudevent was called twice."""

    @pytest.mark.asyncio
    async def test_emit_transient_then_redelivered_emits_once_with_same_id(
        self,
    ) -> None:
        from azure.core.exceptions import HttpResponseError

        work_item = _default_work_item()

        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(100, "a" * 64, 10))
        mock_adls.write_json = AsyncMock()
        mock_adls.blob_url = AsyncMock(return_value="http://azurite/bronze/x.csv")
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        # No completed version exists across either attempt — that's the
        # whole point: mark_completed never ran after attempt 1.
        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_failed = AsyncMock()
        mock_tracker.mark_completed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        transient_cause = HttpResponseError(message="unavailable")
        transient_cause.status_code = 503

        emitter = _mock_emitter()
        emitter.emit_cloudevent = AsyncMock(side_effect=transient_cause)

        # --- Attempt 1: emit_cloudevent fails transiently, worker raises ---
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            with pytest.raises(HttpResponseError):
                await historical_worker(_mock_work_item_msg(work_item))

        mock_tracker.mark_completed.assert_not_called()
        first_envelope = emitter.emit_cloudevent.call_args.kwargs["envelope"]
        first_id = first_envelope["id"]
        first_version = first_envelope["data"]["version"]

        # --- Attempt 2 (redelivery of the identical message): succeeds ---
        emitter.emit_cloudevent = AsyncMock(return_value=None)

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(_mock_work_item_msg(work_item))  # must NOT raise

        mock_tracker.mark_completed.assert_called_once()
        second_envelope = emitter.emit_cloudevent.call_args.kwargs["envelope"]

        # Same file identity + version resolved both times -> same id.
        assert second_envelope["data"]["version"] == first_version
        assert second_envelope["id"] == first_id


class TestOneTryRegionCoversEveryCallSite:
    """R2.1d item 3: a single try/except region covers load_historical_config,
    store construction, get_versions, mark_processing, upload, emit, and
    mark_completed. Classification happens in exactly one place, and every
    failure sets stats.datasets_failed — table-driven over
    call site x {transient, deterministic}.

    config-load failure is the one exception: there is no config yet to
    dead-letter with, so it always raises regardless of the exception type,
    and is tested separately below rather than in the shared table.
    """

    def _mocks(self) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
        mock_adls = AsyncMock()
        mock_adls.stream_upload = AsyncMock(return_value=(100, "a" * 64, 10))
        mock_adls.write_json = AsyncMock()
        mock_adls.blob_url = AsyncMock(return_value="http://azurite/bronze/x.csv")
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.mark_processing = AsyncMock()
        mock_tracker.mark_completed = AsyncMock()
        mock_tracker.mark_failed = AsyncMock()
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()
        return mock_adls, mock_tracker, emitter

    async def _run(self, call_site: str, exc: Exception):
        """Run historical_worker with *exc* injected at *call_site*.

        Returns (raised_exception_or_None, captured_stats, mock_tracker,
        emitter).
        """
        from src.observability import DatasetIngestionStats

        mock_adls, mock_tracker, emitter = self._mocks()

        if call_site == "store_construction":
            tracker_factory = MagicMock(side_effect=exc)
        else:
            tracker_factory = MagicMock(return_value=mock_tracker)

        if call_site == "get_versions":
            mock_tracker.get_versions = AsyncMock(side_effect=exc)
        elif call_site == "mark_processing":
            mock_tracker.mark_processing = AsyncMock(side_effect=exc)
        elif call_site == "upload":
            mock_adls.stream_upload = AsyncMock(side_effect=exc)
        elif call_site == "emit":
            emitter.emit_cloudevent = AsyncMock(side_effect=exc)
        elif call_site == "mark_completed":
            mock_tracker.mark_completed = AsyncMock(side_effect=exc)

        captured_stats: list[DatasetIngestionStats] = []

        def _capture_metrics(stats: DatasetIngestionStats) -> dict:
            captured_stats.append(stats)
            return {}

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        raised = None
        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", tracker_factory),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics", side_effect=_capture_metrics),
        ):
            from function_app import historical_worker

            try:
                await historical_worker(mock_msg)
            except Exception as caught:  # noqa: BLE001 - capturing for assertion
                raised = caught

        return raised, captured_stats, mock_tracker, emitter

    _CALL_SITES = [
        "store_construction",
        "get_versions",
        "mark_processing",
        "upload",
        "emit",
        "mark_completed",
    ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call_site", _CALL_SITES)
    async def test_transient_failure_reraises_dead_letters_and_sets_metric(
        self, call_site: str
    ) -> None:
        from azure.core.exceptions import ServiceRequestError

        exc = ServiceRequestError("connection reset")
        raised, captured_stats, _tracker, emitter = await self._run(call_site, exc)

        assert raised is exc, f"{call_site}: transient failure must re-raise"
        emitter.emit_dead_letter.assert_called_once()
        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("call_site", _CALL_SITES)
    async def test_deterministic_failure_completes_dead_letters_and_sets_metric(
        self, call_site: str
    ) -> None:
        exc = ValueError(f"deterministic bug at {call_site}")
        raised, captured_stats, _tracker, emitter = await self._run(call_site, exc)

        assert raised is None, f"{call_site}: deterministic failure must not raise"
        emitter.emit_dead_letter.assert_called_once()
        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_config_load_failure_always_raises_never_dead_letters(self) -> None:
        """No config exists yet to dead-letter with — this is the one call
        site excluded from classification: it always re-raises, whatever
        the exception, and metrics are still emitted with datasets_failed set."""
        from src.observability import DatasetIngestionStats

        exc = RuntimeError("bad config")
        captured_stats: list[DatasetIngestionStats] = []

        def _capture_metrics(stats: DatasetIngestionStats) -> dict:
            captured_stats.append(stats)
            return {}

        emitter = _mock_emitter()
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        with (
            patch("function_app.load_historical_config", side_effect=exc),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics", side_effect=_capture_metrics),
        ):
            from function_app import historical_worker

            with pytest.raises(RuntimeError, match="bad config"):
                await historical_worker(mock_msg)

        emitter.emit_dead_letter.assert_not_called()
        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_success_path_sets_no_failed_metric(self) -> None:
        from src.observability import DatasetIngestionStats

        mock_adls, mock_tracker, emitter = self._mocks()
        captured_stats: list[DatasetIngestionStats] = []

        def _capture_metrics(stats: DatasetIngestionStats) -> dict:
            captured_stats.append(stats)
            return {}

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
            patch("function_app.emit_dataset_metrics", side_effect=_capture_metrics),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 0
        assert captured_stats[0].datasets_emitted == 1
        emitter.emit_dead_letter.assert_not_called()


class TestDeadLetterSendFailureDoesNotRetry:
    """R2.1e item 2: if the dead-letter send itself fails, _dead_letter
    raises DeadLetterSendError, and historical_worker must not attempt a
    second dead-letter send for the same delivery — it escapes instead so
    Service Bus redelivers. The reviewer's scenario: a malformed body (the
    first thing that can trigger a dead-letter send), whose first
    emit_dead_letter call fails; a second call, if attempted, would
    succeed — proving this is about NOT retrying, not about the retry
    being doomed."""

    @pytest.mark.asyncio
    async def test_malformed_body_dead_letter_send_failure_escapes_without_retry(
        self,
    ) -> None:
        from function_app import DeadLetterSendError

        mock_msg = MagicMock()
        mock_msg.get_body.return_value = b"this is not json {"

        emitter = _mock_emitter()
        emitter.emit_dead_letter = AsyncMock(
            side_effect=[RuntimeError("transient send failure"), None]
        )

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            with pytest.raises(DeadLetterSendError):
                await historical_worker(mock_msg)

        # Exactly one dead-letter attempt per delivery — the failed one.
        # historical_worker must not have tried again with the same reason.
        emitter.emit_dead_letter.assert_called_once()
        body = emitter.emit_dead_letter.call_args.kwargs["message_body"]
        assert body["error_type"] == "malformed_body"


class TestFullCallOrder:
    """R2.1e item 3: asserts the exact end-to-end call order across the
    four guarded operations. Manually verified as mutation-sensitive: with
    function_app.py locally edited to call write_json after emit_cloudevent
    instead of before, this test fails (call_order comes back as
    [stream_upload, emit_cloudevent, write_json, mark_completed]); reverting
    the edit makes it pass again — see the R2.1e report for the exact
    mutation and result."""

    @pytest.mark.asyncio
    async def test_call_order_is_upload_write_json_emit_complete(self) -> None:
        call_order: list[str] = []

        mock_adls = AsyncMock()

        async def _stream_upload(*args, **kwargs):
            call_order.append("stream_upload")
            return (100, "a" * 64, 10)

        async def _write_json(*args, **kwargs):
            call_order.append("write_json")

        mock_adls.stream_upload = _stream_upload
        mock_adls.write_json = _write_json
        mock_adls.blob_url = AsyncMock(return_value="http://azurite/bronze/x.csv")
        mock_adls.__aenter__ = AsyncMock(return_value=mock_adls)
        mock_adls.__aexit__ = AsyncMock(return_value=False)

        mock_tracker = AsyncMock()
        mock_tracker.get_versions = AsyncMock(return_value=[])
        mock_tracker.mark_processing = AsyncMock()

        async def _mark_completed(*args, **kwargs):
            call_order.append("mark_completed")

        mock_tracker.mark_completed = _mark_completed
        mock_tracker.__aenter__ = AsyncMock(return_value=mock_tracker)
        mock_tracker.__aexit__ = AsyncMock(return_value=False)

        emitter = _mock_emitter()

        async def _emit_cloudevent(*args, **kwargs):
            call_order.append("emit_cloudevent")

        emitter.emit_cloudevent = _emit_cloudevent

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch("function_app.AdlsStore", return_value=mock_adls),
            patch("function_app.FileTrackingStore", return_value=mock_tracker),
            patch("function_app.ServiceBusEmitter", return_value=emitter),
            patch("function_app.create_logger", return_value=MagicMock()),
        ):
            from function_app import historical_worker

            await historical_worker(mock_msg)

        assert call_order == [
            "stream_upload",
            "write_json",
            "emit_cloudevent",
            "mark_completed",
        ]


class TestGuardedRegionCoversInitialSteps:
    """R2.1e item 5: msg.get_body(), create_logger, and stats construction
    are themselves inside the guarded try region (stats constructed first,
    so the `finally` can always report through it) — a failure in any of
    them is handled the same way a config-load failure is (no config yet
    to dead-letter with, so it always escapes), not an uncaught crash
    outside the try/finally."""

    @pytest.mark.asyncio
    async def test_get_body_failure_raises(self) -> None:
        mock_msg = MagicMock()
        mock_msg.get_body.side_effect = RuntimeError("body unreadable")

        captured_stats: list = []

        with patch(
            "function_app.emit_dataset_metrics",
            side_effect=lambda s: captured_stats.append(s),
        ):
            from function_app import historical_worker

            with pytest.raises(RuntimeError, match="body unreadable"):
                await historical_worker(mock_msg)

        # stats is constructed before get_body() is even called, so the
        # `finally` must still be able to report through it — exactly once,
        # with the failure counted.
        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_create_logger_failure_raises_and_logs_via_fallback(
        self, caplog
    ) -> None:
        import logging

        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        captured_stats: list = []

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch(
                "function_app.create_logger",
                side_effect=RuntimeError("logger setup failed"),
            ),
            patch(
                "function_app.emit_dataset_metrics",
                side_effect=lambda s: captured_stats.append(s),
            ),
            caplog.at_level(logging.ERROR, logger="function_app"),
        ):
            from function_app import historical_worker

            with pytest.raises(RuntimeError, match="logger setup failed"):
                await historical_worker(mock_msg)

        # The fallback logger (plain logging.getLogger(__name__), not
        # create_logger's LoggerAdapter) is what reports this — proving the
        # except/finally blocks never crash for want of a working logger.
        assert any(
            "before config could be loaded" in record.message
            for record in caplog.records
        )
        assert len(captured_stats) == 1
        assert captured_stats[0].datasets_failed == 1

    @pytest.mark.asyncio
    async def test_stats_construction_failure_raises_and_skips_metrics(self) -> None:
        work_item = _default_work_item()
        mock_msg = _mock_work_item_msg(work_item)

        captured_calls: list = []

        with (
            patch(
                "function_app.load_historical_config", return_value=_historical_config()
            ),
            patch(
                "function_app.DatasetIngestionStats",
                side_effect=RuntimeError("stats ctor failed"),
            ),
            patch(
                "function_app.emit_dataset_metrics",
                side_effect=lambda s: captured_calls.append(s),
            ),
        ):
            from function_app import historical_worker

            with pytest.raises(RuntimeError, match="stats ctor failed"):
                await historical_worker(mock_msg)

        # No stats object was ever successfully constructed — nothing to
        # emit metrics for, and no AttributeError/NameError either.
        assert captured_calls == []
