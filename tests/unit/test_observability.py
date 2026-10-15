"""Unit tests for structured observability.

Tests cover: structured log context fields and exception formatting.
DatasetIngestionStats / emit_dataset_metrics (feature 002) are covered by
tests/unit/test_file_tracking_store.py and the historical pipeline
integration tests. InvocationStats / emit_invocation_metrics /
emit_warning_metric (feature 001 only) were retired in R2.9 along with
pvdaq_ingestion.
"""

from __future__ import annotations

import io
import json
import logging

from src.observability import _JsonFormatter, create_logger


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
            function_name="historical_worker",
        )

        assert logger.extra["correlation_id"] == "test-corr-id"
        assert logger.extra["function_name"] == "historical_worker"
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

    def test_log_record_emits_correlation_context_as_json(self) -> None:
        """R2.9c item 2: drives _CorrelatedAdapter.process's extras
        injection (observability.py:51-56) through an actual logger.info()
        call, and _JsonFormatter's hasattr branches that pick those extras
        back up (observability.py:35,37,39) — previously 0% coverage
        because every other caller either reads ``logger.extra`` directly
        (never logs through it) or patches create_logger with a MagicMock."""
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(_JsonFormatter())

        logger = create_logger(
            correlation_id="corr-xyz", vendor="PVDAQ", function_name="historical_worker"
        )
        logger.logger.addHandler(handler)
        try:
            logger.info("doing work")
        finally:
            logger.logger.removeHandler(handler)

        emitted = json.loads(stream.getvalue().strip())
        assert emitted["correlation_id"] == "corr-xyz"
        assert emitted["function_name"] == "historical_worker"
        assert emitted["vendor"] == "PVDAQ"
        assert emitted["message"] == "doing work"
