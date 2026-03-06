"""Contract test: CloudEvents envelopes conform to cloudevents-envelope.json schema."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from src.cloudevents_envelope import build_envelope
from src.config import Config

_CE_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "specs"
    / "001-pvdaq-ingestion"
    / "contracts"
    / "cloudevents-envelope.json"
)
_CE_SCHEMA: dict = json.loads(_CE_SCHEMA_PATH.read_text(encoding="utf-8"))

_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class TestCloudEventsContract:
    """CloudEvents envelope matches the published contract schema."""

    def test_envelope_validates_against_contract(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        jsonschema.validate(instance=envelope, schema=_CE_SCHEMA)

    def test_specversion_is_1_0(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["specversion"] == "1.0"

    def test_type_is_raw_pvdaq_generation_v1(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["type"] == "raw.pvdaq.generation.v1"

    def test_source_is_energy_ingestion_boundary_pvdaq(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["source"] == "/energy-ingestion-boundary/pvdaq"

    def test_datacontenttype_is_application_json(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["datacontenttype"] == "application/json"

    def test_extension_attributes_present(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["tenant_id"] == mock_config.tenant_id
        assert envelope["source_vendor"] == "PVDAQ"
        assert envelope["schema_version"] == mock_config.schema_version_pvdaq
        assert envelope["mapping_version"] == mock_config.mapping_version_pvdaq
        assert envelope["correlation_id"] == correlation_id
        assert "ingestion_timestamp" in envelope
        assert envelope["traceparent"] == _TRACEPARENT

    def test_data_equals_original_payload(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["data"] == sample_valid_record
