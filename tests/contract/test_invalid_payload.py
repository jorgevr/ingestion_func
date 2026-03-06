"""Contract test: dead-letter messages conform to dead-letter-message.json schema."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from src.schema_validator import validate_record
from src.service_bus_emitter import build_dead_letter_message

_DLQ_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "specs"
    / "001-pvdaq-ingestion"
    / "contracts"
    / "dead-letter-message.json"
)
_DLQ_SCHEMA: dict = json.loads(_DLQ_SCHEMA_PATH.read_text(encoding="utf-8"))


class TestDeadLetterContract:
    """Dead-letter message structure matches the published contract schema."""

    def test_malformed_record_produces_valid_dead_letter(
        self, sample_invalid_record: dict, correlation_id: str
    ) -> None:
        # Validate the record to get error details
        is_valid, error_details = validate_record(sample_invalid_record)
        assert is_valid is False

        # Build the dead-letter message
        dlq_message = build_dead_letter_message(
            original_payload=sample_invalid_record,
            error_details=error_details,
            correlation_id=correlation_id,
            site_id=sample_invalid_record.get("SiteID"),
            schema_version="v1",
        )

        # Validate against the contract schema
        jsonschema.validate(instance=dlq_message, schema=_DLQ_SCHEMA)

    def test_original_payload_preserved(
        self, sample_invalid_record: dict, correlation_id: str
    ) -> None:
        _, error_details = validate_record(sample_invalid_record)

        dlq_message = build_dead_letter_message(
            original_payload=sample_invalid_record,
            error_details=error_details,
            correlation_id=correlation_id,
            site_id=sample_invalid_record.get("SiteID"),
            schema_version="v1",
        )

        assert dlq_message["original_payload"] == sample_invalid_record

    def test_error_details_populated(
        self, sample_invalid_record: dict, correlation_id: str
    ) -> None:
        _, error_details = validate_record(sample_invalid_record)

        dlq_message = build_dead_letter_message(
            original_payload=sample_invalid_record,
            error_details=error_details,
            correlation_id=correlation_id,
            site_id=sample_invalid_record.get("SiteID"),
            schema_version="v1",
        )

        assert len(dlq_message["error_details"]) >= 1
        for detail in dlq_message["error_details"]:
            assert "message" in detail
            assert "path" in detail

    def test_correlation_id_present(
        self, sample_invalid_record: dict, correlation_id: str
    ) -> None:
        _, error_details = validate_record(sample_invalid_record)

        dlq_message = build_dead_letter_message(
            original_payload=sample_invalid_record,
            error_details=error_details,
            correlation_id=correlation_id,
            site_id=sample_invalid_record.get("SiteID"),
            schema_version="v1",
        )

        assert dlq_message["correlation_id"] == correlation_id
