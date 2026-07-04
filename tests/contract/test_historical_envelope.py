"""Contract test: Historical CloudEvents envelopes conform to expected schema."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from src.cloudevents_envelope import build_envelope
from src.config import HistoricalConfig

_CE_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "specs"
    / "001-pvdaq-ingestion"
    / "contracts"
    / "cloudevents-envelope.json"
)
_CE_SCHEMA: dict = json.loads(_CE_SCHEMA_PATH.read_text(encoding="utf-8"))

_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
_CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000"


def _historical_config() -> HistoricalConfig:
    return HistoricalConfig(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_historical_prefix="pvdaq/2023-solar-data-prize",
        pvdaq_historical_site_ids=[9068, 9069, 2107, 7333],
        pvdaq_historical_cron_schedule="0 0 */6 * * *",
        pvdaq_historical_queue_name="pvdaq-historical-work",
        service_bus_queue_name="raw-energy-events",
        dead_letter_queue_name="pvdaq-dead-letter",
        service_bus_fully_qualified_namespace="test-sb.servicebus.windows.net",
        file_tracking_table_name="PvdaqFileTracking",
        table_storage_uri="https://teststorage.table.core.windows.net",
        adls_account_url="https://testaccount.dfs.core.windows.net",
        adls_container_name="raw",
        tenant_id="default",
        mapping_version_pvdaq="unknown",
        schema_version_pvdaq="v1",
    )


def _sample_record() -> dict:
    return {
        "SiteID": 9068,
        "measdatetime": "2023-06-15T12:00:00",
        "ac_power": 4500.0,
        "ambient_temperature": 25.3,
    }


class TestHistoricalEnvelopeContract:
    """Historical CloudEvents envelope with parameterized type and source."""

    def test_envelope_validates_against_contract(self) -> None:
        envelope = build_envelope(
            record=_sample_record(),
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
            event_type="raw.pvdaq.historical.v1",
            source="/energy-ingestion-boundary/pvdaq-historical",
        )
        jsonschema.validate(instance=envelope, schema=_CE_SCHEMA)

    def test_type_is_raw_pvdaq_historical_v1(self) -> None:
        envelope = build_envelope(
            record=_sample_record(),
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
            event_type="raw.pvdaq.historical.v1",
            source="/energy-ingestion-boundary/pvdaq-historical",
        )
        assert envelope["type"] == "raw.pvdaq.historical.v1"

    def test_source_is_pvdaq_historical(self) -> None:
        envelope = build_envelope(
            record=_sample_record(),
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
            event_type="raw.pvdaq.historical.v1",
            source="/energy-ingestion-boundary/pvdaq-historical",
        )
        assert envelope["source"] == "/energy-ingestion-boundary/pvdaq-historical"

    def test_extension_attributes_present(self) -> None:
        envelope = build_envelope(
            record=_sample_record(),
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
            event_type="raw.pvdaq.historical.v1",
            source="/energy-ingestion-boundary/pvdaq-historical",
        )
        assert envelope["source_vendor"] == "PVDAQ"
        assert envelope["schema_version"] == "v1"
        assert envelope["correlation_id"] == _CORRELATION_ID
        assert "ingestion_timestamp" in envelope
        assert envelope["traceparent"] == _TRACEPARENT

    def test_data_equals_original_record(self) -> None:
        record = _sample_record()
        envelope = build_envelope(
            record=record,
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
            event_type="raw.pvdaq.historical.v1",
            source="/energy-ingestion-boundary/pvdaq-historical",
        )
        assert envelope["data"] == record

    def test_default_type_backward_compatible(self) -> None:
        """Without explicit event_type, defaults to feature 001 type."""
        envelope = build_envelope(
            record=_sample_record(),
            config=_historical_config(),
            correlation_id=_CORRELATION_ID,
            traceparent=_TRACEPARENT,
        )
        assert envelope["type"] == "raw.pvdaq.generation.v1"
        assert envelope["source"] == "/energy-ingestion-boundary/pvdaq"
