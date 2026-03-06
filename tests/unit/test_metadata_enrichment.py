"""Unit tests for src.cloudevents_envelope — metadata enrichment."""

from __future__ import annotations

import copy
import uuid

from src.cloudevents_envelope import build_envelope
from src.config import Config

_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class TestEnvelopeRequiredFields:
    """The envelope contains all required CloudEvents and extension fields."""

    def test_has_all_required_fields(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        required = [
            "specversion", "type", "source", "id", "time",
            "datacontenttype", "tenant_id", "source_vendor",
            "schema_version", "mapping_version", "correlation_id",
            "ingestion_timestamp", "traceparent", "data",
        ]
        for field in required:
            assert field in envelope, f"Missing required field: {field}"


class TestMappingVersionDefault:
    """mapping_version reflects config value including the 'unknown' sentinel."""

    def test_mapping_version_defaults_to_unknown(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        # mock_config has mapping_version_pvdaq="unknown"
        assert mock_config.mapping_version_pvdaq == "unknown"

        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["mapping_version"] == "unknown"


class TestCorrelationId:
    """correlation_id is a valid UUID string."""

    def test_correlation_id_is_valid_uuid(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        # Should not raise
        uuid.UUID(envelope["correlation_id"])


class TestIngestionTimestamp:
    """ingestion_timestamp is a UTC ISO-8601 string."""

    def test_ingestion_timestamp_is_utc_iso8601(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        ts = envelope["ingestion_timestamp"]
        assert isinstance(ts, str)
        # Must contain UTC offset indicator
        assert "+" in ts or "Z" in ts or ts.endswith("+00:00")


class TestPayloadImmutability:
    """The original record is not mutated by envelope construction."""

    def test_original_payload_not_mutated(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        original = copy.deepcopy(sample_valid_record)

        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        # Mutate the envelope data to prove independence
        envelope["data"]["SiteID"] = 9999

        assert sample_valid_record == original


class TestUniqueId:
    """Each envelope gets a unique UUID id."""

    def test_id_is_unique_per_call(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        e1 = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )
        e2 = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert e1["id"] != e2["id"]
        # Both should be valid UUIDs
        uuid.UUID(e1["id"])
        uuid.UUID(e2["id"])


class TestTraceparent:
    """traceparent is included in the envelope."""

    def test_traceparent_included(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        assert envelope["traceparent"] == _TRACEPARENT
