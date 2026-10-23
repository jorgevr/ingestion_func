"""Structured logging and observability for PVDAQ ingestion function.

Emits JSON-formatted log entries that include correlation_id, function_name,
and vendor in every message.  Provides a factory function to create
preconfigured LoggerAdapter instances, plus custom metrics for FR-011
(dataset-level) telemetry.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


class _JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge extra fields injected via LoggerAdapter
        if hasattr(record, "correlation_id"):
            log_entry["correlation_id"] = record.correlation_id  # type: ignore[attr-defined]
        if hasattr(record, "function_name"):
            log_entry["function_name"] = record.function_name  # type: ignore[attr-defined]
        if hasattr(record, "vendor"):
            log_entry["vendor"] = record.vendor  # type: ignore[attr-defined]

        # Include exception info when present
        if record.exc_info and record.exc_info[1] is not None:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class _CorrelatedAdapter(logging.LoggerAdapter):
    """LoggerAdapter that injects correlation context into every log record."""

    def process(
        self, msg: str, kwargs: MutableMapping[str, Any]
    ) -> tuple[str, MutableMapping[str, Any]]:
        extra = kwargs.setdefault("extra", {})
        extra.update(self.extra)  # type: ignore[arg-type]
        return msg, kwargs


_handler_installed = False


def _ensure_handler() -> None:
    """Install the JSON handler on the root logger exactly once."""
    global _handler_installed
    if _handler_installed:
        return

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(_JsonFormatter())

    root = logging.getLogger()
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    _handler_installed = True


def create_logger(
    correlation_id: str,
    vendor: str = "PVDAQ",
    function_name: str = "historical_worker",
) -> logging.LoggerAdapter:
    """Create a structured logger with correlation context.

    Args:
        correlation_id: Unique identifier for tracing a single invocation.
        vendor: Data vendor name (default ``"PVDAQ"``).
        function_name: Name of the Azure Function (default ``"historical_worker"``).

    Returns:
        A ``logging.LoggerAdapter`` that injects *correlation_id*,
        *function_name*, and *vendor* into every JSON log entry.
    """
    _ensure_handler()

    logger = logging.getLogger(f"ingestion.{vendor.lower()}")
    return _CorrelatedAdapter(
        logger,
        {
            "correlation_id": correlation_id,
            "function_name": function_name,
            "vendor": vendor,
        },
    )


_metrics_logger = logging.getLogger("ingestion.metrics")


@dataclass
class DatasetIngestionStats:
    """Accumulates dataset-level counters for a single historical ingestion run.

    Used by ``emit_dataset_metrics`` to produce the FR-011 telemetry summary
    at the end of each dispatcher or worker invocation.
    """

    source: str
    correlation_id: str
    datasets_discovered: int = 0
    datasets_downloaded: int = 0
    datasets_stored: int = 0
    datasets_emitted: int = 0
    datasets_failed: int = 0
    duration_ms: float = 0.0


def emit_dataset_metrics(stats: DatasetIngestionStats) -> dict[str, Any]:
    """Log a structured telemetry summary for a dataset ingestion run.

    Args:
        stats: Accumulated dataset-level counters.

    Returns:
        The metrics dict (for testing / further processing).
    """
    metrics: dict[str, Any] = {
        "source": stats.source,
        "datasets_discovered": stats.datasets_discovered,
        "datasets_downloaded": stats.datasets_downloaded,
        "datasets_stored": stats.datasets_stored,
        "datasets_emitted": stats.datasets_emitted,
        "datasets_failed": stats.datasets_failed,
        "duration_ms": stats.duration_ms,
        "correlation_id": stats.correlation_id,
    }
    _metrics_logger.info("Dataset metrics: %s", json.dumps(metrics, default=str))
    return metrics
