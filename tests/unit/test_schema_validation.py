"""Unit tests for src.schema_validator — PVDAQ record validation."""

from __future__ import annotations

from src.schema_validator import validate_record


class TestValidRecord:
    """A well-formed PVDAQ record passes validation."""

    def test_valid_record_passes(self, sample_valid_record: dict) -> None:
        is_valid, errors = validate_record(sample_valid_record)
        assert is_valid is True
        assert errors == []


class TestMissingRequiredFields:
    """Missing required fields are reported as validation errors."""

    def test_missing_site_id_fails(self, sample_valid_record: dict) -> None:
        record = {k: v for k, v in sample_valid_record.items() if k != "SiteID"}
        is_valid, errors = validate_record(record)

        assert is_valid is False
        assert len(errors) >= 1
        messages = " ".join(e["message"] for e in errors)
        assert "SiteID" in messages

    def test_missing_measdatetime_fails(self, sample_valid_record: dict) -> None:
        record = {k: v for k, v in sample_valid_record.items() if k != "measdatetime"}
        is_valid, errors = validate_record(record)

        assert is_valid is False
        assert len(errors) >= 1
        messages = " ".join(e["message"] for e in errors)
        assert "measdatetime" in messages


class TestWrongType:
    """Type mismatches are detected."""

    def test_string_site_id_fails(self, sample_valid_record: dict) -> None:
        record = {**sample_valid_record, "SiteID": "not-an-int"}
        is_valid, errors = validate_record(record)

        assert is_valid is False
        assert any(e["validator"] == "type" for e in errors)


class TestAdditionalProperties:
    """Extra fields are allowed (additionalProperties: true)."""

    def test_extra_fields_allowed(self, sample_valid_record: dict) -> None:
        record = {**sample_valid_record, "custom_field": "hello"}
        is_valid, errors = validate_record(record)

        assert is_valid is True
        assert errors == []


class TestMultipleErrors:
    """Multiple errors are collected in a single validation pass."""

    def test_missing_site_id_and_measdatetime(self) -> None:
        record = {"ac_power": 100.0}
        is_valid, errors = validate_record(record)

        assert is_valid is False
        assert len(errors) >= 2
        messages = " ".join(e["message"] for e in errors)
        assert "SiteID" in messages
        assert "measdatetime" in messages
