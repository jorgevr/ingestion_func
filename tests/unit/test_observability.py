"""Unit tests for structured observability (T032).

Tests cover: InvocationStats dataclass, emit_invocation_metrics() output,
mapping_version warning metric, and structured log context fields.
"""

from __future__ import annotations

import logging

import pytest

from src.observability import (
    InvocationStats,
    create_logger,
    emit_invocation_metrics,
    emit_warning_metric,
)


class TestInvocationStats:
    """InvocationStats dataclass accumulates pipeline counters."""

    def test_default_values(self) -> None:
        stats = InvocationStats(source="PVDAQ", correlation_id="test-corr-id")
        assert stats.source == "PVDAQ"
        assert stats.number_of_records_retrieved == 0
        assert stats.number_valid == 0
        assert stats.number_invalid == 0
        assert stats.number_emitted == 0
        assert stats.number_duplicates == 0
        assert stats.duration_ms == 0.0
        assert stats.correlation_id == "test-corr-id"

    def test_increment_counters(self) -> None:
        stats = InvocationStats(source="PVDAQ", correlation_id="test-corr-id")
        stats.number_of_records_retrieved = 10
        stats.number_valid = 8
        stats.number_invalid = 2
        stats.number_emitted = 8
        stats.duration_ms = 1234.5

        assert stats.number_of_records_retrieved == 10
        assert stats.number_emitted == 8


    def test_historical_source(self) -> None:
        stats = InvocationStats(source="PVDAQ-historical", correlation_id="hist-corr")
        assert stats.source == "PVDAQ-historical"


class TestEmitInvocationMetrics:
    """emit_invocation_metrics() produces dict with all FR-009 fields."""

    def test_returns_dict_with_all_fields(self) -> None:
        stats = InvocationStats(
            source="PVDAQ",
            correlation_id="test-corr-id",
            number_of_records_retrieved=10,
            number_valid=8,
            number_invalid=2,
            number_emitted=8,
            duration_ms=1234.5,
        )

        result = emit_invocation_metrics(stats)

        assert result["source"] == "PVDAQ"
        assert result["number_of_records_retrieved"] == 10
        assert result["number_valid"] == 8
        assert result["number_invalid"] == 2
        assert result["number_emitted"] == 8
        assert result["duration_ms"] == 1234.5
        assert result["correlation_id"] == "test-corr-id"

    def test_includes_number_duplicates(self) -> None:
        stats = InvocationStats(
            source="PVDAQ",
            correlation_id="test-corr-id",
            number_duplicates=3,
        )

        result = emit_invocation_metrics(stats)
        assert result["number_duplicates"] == 3


class TestWarningMetric:
    """emit_warning_metric() logs a warning with metric details."""

    def test_emits_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            emit_warning_metric(
                metric_name="mapping_version_unknown",
                details={"mapping_version": "unknown", "vendor": "PVDAQ"},
            )

        assert len(caplog.records) >= 1
        warning_record = caplog.records[-1]
        assert warning_record.levelname == "WARNING"
        assert "mapping_version_unknown" in warning_record.getMessage()


class TestExceptionFormatting:
    """_JsonFormatter includes exception info when present."""

    def test_exception_included_in_json_output(self) -> None:
        """When a log record has exc_info, the JSON output includes 'exception'."""
        import sys
        from src.observability import _JsonFormatter

        formatter = _JsonFormatter()

        try:
            raise ValueError("test error for coverage")
        except ValueError:
            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="Something failed",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        assert '"exception"' in output
        assert "ValueError" in output
        assert "test error for coverage" in output


class TestStructuredLogContext:
    """Structured log entries include correlation_id, function_name, vendor."""

    def test_logger_adapter_has_context(self) -> None:
        logger = create_logger(
            correlation_id="test-corr-id",
            vendor="PVDAQ",
            function_name="pvdaq_ingestion",
        )

        assert logger.extra["correlation_id"] == "test-corr-id"
        assert logger.extra["function_name"] == "pvdaq_ingestion"
        assert logger.extra["vendor"] == "PVDAQ"

    def test_historical_worker_context(self) -> None:
        logger = create_logger(
            correlation_id="hist-corr",
            vendor="PVDAQ",
            function_name="historical_worker",
        )

        assert logger.extra["function_name"] == "historical_worker"
        assert logger.extra["vendor"] == "PVDAQ"

    def test_historical_dispatcher_context(self) -> None:
        logger = create_logger(
            correlation_id="disp-corr",
            vendor="PVDAQ",
            function_name="historical_dispatcher",
        )

        assert logger.extra["function_name"] == "historical_dispatcher"
