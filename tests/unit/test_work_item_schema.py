"""Unit tests for schemas/work-item.v1.json's pattern constraints.

jsonschema's optional date-time/uuid format packages are deliberately not
installed in this repo (see schemas/README.md and
tests/unit/test_storage_switch.py's equivalent note for the registry
contracts) — validation must be deterministic on `pattern` alone, whether
or not format assertion happens to be active. These tests build the
validator the exact way function_app.py does (no FormatChecker passed) so
they'd catch a regression to format-only validation.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, ValidationError

_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent / "schemas" / "work-item.v1.json"
)
_SCHEMA: dict = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
_VALIDATOR = Draft202012Validator(
    _SCHEMA
)  # no format_checker — matches function_app.py


def _valid_work_item(**overrides) -> dict:
    base = {
        "site_id": 9068,
        "s3_key": "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_data.csv",
        "file_name": "9068_ac_power_data.csv",
        "category": "ac_power",
        "correlation_id": "550e8400-e29b-41d4-a716-446655440000",
        "enqueued_at": "2024-01-15T12:00:00Z",
        "last_modified": "2024-01-15T12:00:00Z",
    }
    base.update(overrides)
    return base


class TestValidWorkItemPasses:
    def test_baseline_valid_work_item_passes(self) -> None:
        _VALIDATOR.validate(_valid_work_item())

    def test_valid_with_timezone_offset_passes(self) -> None:
        _VALIDATOR.validate(_valid_work_item(enqueued_at="2024-01-15T12:00:00+00:00"))

    def test_valid_without_optional_last_modified_passes(self) -> None:
        item = _valid_work_item()
        del item["last_modified"]
        _VALIDATOR.validate(item)


class TestCorrelationIdPatternRejectsNonUuid:
    @pytest.mark.parametrize(
        "bad_value",
        [
            "test-corr-id",
            "not-a-uuid-at-all",
            "550E8400-E29B-41D4-A716-446655440000",  # uppercase — pattern requires lowercase
            "550e8400e29b41d4a716446655440000",  # missing hyphens
            "",
        ],
    )
    def test_rejected(self, bad_value: str) -> None:
        with pytest.raises(ValidationError):
            _VALIDATOR.validate(_valid_work_item(correlation_id=bad_value))


class TestEnqueuedAtPatternRejectsMalformedDateTime:
    """The pattern validates shape (digits/separators), not calendar
    semantics — an out-of-range month or hour is a separate, harder problem
    (needs a real date parser, i.e. format assertion) this pattern does not
    claim to catch. It reliably rejects garbage shapes."""

    @pytest.mark.parametrize(
        "bad_value",
        [
            "",
            "not-a-date",
            "2024-01-15",  # date only, no time
            "2024/01/15T12:00:00Z",  # wrong separators
            "2024-01-15 12:00:00",  # space instead of T, no offset
        ],
    )
    def test_rejected(self, bad_value: str) -> None:
        with pytest.raises(ValidationError):
            _VALIDATOR.validate(_valid_work_item(enqueued_at=bad_value))


class TestLastModifiedPatternAndMinLength:
    def test_empty_string_rejected_by_min_length(self) -> None:
        with pytest.raises(ValidationError):
            _VALIDATOR.validate(_valid_work_item(last_modified=""))

    @pytest.mark.parametrize(
        "bad_value",
        ["not-a-date", "2024-01-15", "2024/01/15T12:00:00Z"],
    )
    def test_malformed_date_rejected(self, bad_value: str) -> None:
        with pytest.raises(ValidationError):
            _VALIDATOR.validate(_valid_work_item(last_modified=bad_value))


class TestFormatAssertionIsNotWhatCatchesTheseRegressions:
    """Documents the deliberate choice from tests/unit/test_storage_switch.py:
    no rfc3339-validator/uuid extras are installed or pinned, so
    `format` alone would silently no-op. Pattern is what's load-bearing —
    proven by validating with format assertion forced off and on, getting
    the identical (rejecting) result either way."""

    def test_rejection_holds_regardless_of_format_checker_state(self) -> None:
        no_format_validator = Draft202012Validator(_SCHEMA)  # no format_checker
        with_format_validator = Draft202012Validator(
            _SCHEMA, format_checker=Draft202012Validator.FORMAT_CHECKER
        )

        bad_item = _valid_work_item(correlation_id="not-a-uuid")

        no_format_errors = list(no_format_validator.iter_errors(bad_item))
        with_format_errors = list(with_format_validator.iter_errors(bad_item))

        assert len(no_format_errors) >= 1
        assert len(with_format_errors) >= 1
