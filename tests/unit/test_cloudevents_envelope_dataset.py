"""Unit tests for build_dataset_envelope's deterministic CloudEvent id.

A redelivered work item that gets far enough to re-emit (e.g. a transient
failure struck after the previous attempt's upload but before
mark_completed ran) must resolve the same file identity + version and
therefore produce the same event id — so a downstream consumer can dedupe
by id instead of silently double-processing the same dataset.

The id is derived from ``s3_key`` (the true, always-unique source
identity), not from ``site_id`` + ``category`` — ``category`` is a
*derived*, non-unique grouping (see ``extract_category``), so two genuinely
different S3 objects could otherwise collide on the same id.
"""

from __future__ import annotations

from src.cloudevents_envelope import _deterministic_event_id, build_dataset_envelope
from src.config import HistoricalConfig


def _historical_config() -> HistoricalConfig:
    return HistoricalConfig(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_historical_prefix="pvdaq/2023-solar-data-prize",
        pvdaq_historical_site_ids=[9068],
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


def _data(site_id: int = 9068, category: str = "ac_power", version: int = 1) -> dict:
    return {
        "site_id": site_id,
        "category": category,
        "file_format": "csv",
        "storage_path": "http://azurite/bronze/x.csv",
        "version": version,
        "ingestion_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "source_url": "https://oedi-data-lake.s3.amazonaws.com/x.csv",
        "file_size": 100,
        "file_hash": "a" * 64,
    }


_S3_KEY = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_data.csv"


class TestDeterministicEventId:
    def test_same_identity_same_id(self) -> None:
        assert _deterministic_event_id(_S3_KEY, 1) == _deterministic_event_id(
            _S3_KEY, 1
        )

    def test_different_version_different_id(self) -> None:
        assert _deterministic_event_id(_S3_KEY, 1) != _deterministic_event_id(
            _S3_KEY, 2
        )

    def test_different_s3_key_different_id(self) -> None:
        other_key = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_irradiance.csv"
        assert _deterministic_event_id(_S3_KEY, 1) != _deterministic_event_id(
            other_key, 1
        )

    def test_id_is_a_valid_uuid_string(self) -> None:
        import uuid

        result = _deterministic_event_id(_S3_KEY, 1)
        # Round-trips through UUID() without raising, and is lowercase canonical.
        assert str(uuid.UUID(result)) == result

    def test_two_split_files_with_same_category_yield_different_ids(self) -> None:
        """The review's motivating scenario: a single logical dataset split
        across two distinct S3 objects that both extract to the same
        site_id+category grouping. Keying on s3_key (not site_id+category)
        is what keeps their event ids from colliding."""
        key_a = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part1.csv"
        key_b = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part2.csv"
        assert _deterministic_event_id(key_a, 1) != _deterministic_event_id(key_b, 1)


class TestBuildDatasetEnvelopeUsesDeterministicId:
    def test_two_calls_with_same_data_produce_the_same_envelope_id(self) -> None:
        config = _historical_config()
        envelope1 = build_dataset_envelope(
            data=_data(),
            config=config,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            traceparent="00-" + "0" * 31 + "1-" + "0" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
            s3_key=_S3_KEY,
        )
        # Simulate a redelivered retry: a fresh ingestion_id/timestamp, same
        # file identity + version.
        envelope2 = build_dataset_envelope(
            data=_data(),
            config=config,
            ingestion_id="ffffffff-1111-2222-3333-444444444444",
            traceparent="00-" + "1" * 31 + "1-" + "1" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:05:00+00:00",
            s3_key=_S3_KEY,
        )

        assert envelope1["id"] == envelope2["id"]
        assert envelope1["id"] == _deterministic_event_id(_S3_KEY, 1)

    def test_different_version_produces_different_envelope_id(self) -> None:
        config = _historical_config()
        envelope_v1 = build_dataset_envelope(
            data=_data(version=1),
            config=config,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            traceparent="00-" + "0" * 31 + "1-" + "0" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
            s3_key=_S3_KEY,
        )
        envelope_v2 = build_dataset_envelope(
            data=_data(version=2),
            config=config,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            traceparent="00-" + "0" * 31 + "1-" + "0" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
            s3_key=_S3_KEY,
        )
        assert envelope_v1["id"] != envelope_v2["id"]

    def test_different_s3_key_produces_different_envelope_id(self) -> None:
        """Same site_id+category, two distinct S3 objects (the split-file
        review scenario) — must not collide on the same envelope id."""
        config = _historical_config()
        key_a = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part1.csv"
        key_b = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part2.csv"
        envelope_a = build_dataset_envelope(
            data=_data(),
            config=config,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            traceparent="00-" + "0" * 31 + "1-" + "0" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
            s3_key=key_a,
        )
        envelope_b = build_dataset_envelope(
            data=_data(),
            config=config,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            traceparent="00-" + "0" * 31 + "1-" + "0" * 15 + "1-01",
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
            s3_key=key_b,
        )
        assert envelope_a["id"] != envelope_b["id"]
