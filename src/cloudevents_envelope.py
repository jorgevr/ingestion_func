"""CloudEvents envelope builder for PVDAQ telemetry records.

Constructs a CloudEvents v1.0 envelope with extension attributes for
tenant, vendor, schema, and tracing metadata.
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from uuid import uuid4

from src.config import Config, HistoricalConfig

# Type alias for configs that provide the metadata fields we need
_ConfigLike = Config | HistoricalConfig


def build_dataset_envelope(
    data: dict,
    config: HistoricalConfig,
    ingestion_id: str,
    traceparent: str,
    ingestion_timestamp: str,
) -> dict:
    """Build a CloudEvents v1.0 envelope for a dataset-available event.

    Produces a ``solar.pvdaq.dataset.available`` envelope conforming to
    ``contracts/dataset-event.json``.  ``mapping_version`` is set to
    ``"unknown"`` per Constitution III fallback (no field mapping at dataset
    level).

    Args:
        data: Dataset data block dict (site_id, category, file_format,
            storage_path, ingestion_id, source_url, file_size, file_hash).
        config: Historical config providing tenant_id, schema_version.
        ingestion_id: UUID for this ingestion; used as correlation_id for
            lineage tracing.
        traceparent: W3C Trace Context traceparent header value.
        ingestion_timestamp: ISO-8601 timestamp when the file was processed.

    Returns:
        A dict conforming to the dataset CloudEvents envelope contract.
    """
    return {
        "specversion": "1.0",
        "type": "solar.pvdaq.dataset.available",
        "source": "/energy-ingestion-boundary/pvdaq",
        "id": str(uuid4()),
        "time": ingestion_timestamp,
        "datacontenttype": "application/json",
        "tenant_id": config.tenant_id,
        "source_vendor": "PVDAQ",
        "schema_version": config.schema_version_pvdaq,
        "mapping_version": "unknown",
        "correlation_id": ingestion_id,
        "ingestion_timestamp": ingestion_timestamp,
        "traceparent": traceparent,
        "data": data,
    }


def build_envelope(
    record: dict,
    config: _ConfigLike,
    correlation_id: str,
    traceparent: str,
    event_type: str = "raw.pvdaq.generation.v1",
    source: str = "/energy-ingestion-boundary/pvdaq",
) -> dict:
    """Build a CloudEvents v1.0 envelope wrapping a validated PVDAQ record.

    Args:
        record: The validated PVDAQ telemetry record (will be deep-copied).
        config: Application configuration providing tenant_id, schema_version,
            and mapping_version.
        correlation_id: UUID string for invocation tracing.
        traceparent: W3C Trace Context traceparent header value.
        event_type: CloudEvents ``type`` field. Defaults to feature 001's
            ``"raw.pvdaq.generation.v1"``.
        source: CloudEvents ``source`` field. Defaults to feature 001's
            ``"/energy-ingestion-boundary/pvdaq"``.

    Returns:
        A dict conforming to the CloudEvents envelope contract schema.
    """
    now = datetime.now(timezone.utc).isoformat()

    return {
        # Core CloudEvents attributes
        "specversion": "1.0",
        "type": event_type,
        "source": source,
        "id": str(uuid4()),
        "time": now,
        "datacontenttype": "application/json",
        # Extension attributes
        "tenant_id": config.tenant_id,
        "source_vendor": "PVDAQ",
        "schema_version": config.schema_version_pvdaq,
        "mapping_version": config.mapping_version_pvdaq,
        "correlation_id": correlation_id,
        "ingestion_timestamp": now,
        "traceparent": traceparent,
        # Payload
        "data": copy.deepcopy(record),
    }
