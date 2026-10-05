"""CloudEvents envelope builder for PVDAQ telemetry records.

Constructs a CloudEvents v1.0 envelope with extension attributes for
tenant, vendor, schema, and tracing metadata.
"""

from __future__ import annotations

import copy
import uuid
from datetime import datetime, timezone
from uuid import uuid4

from src.config import Config, HistoricalConfig

# Type alias for configs that provide the metadata fields we need
_ConfigLike = Config | HistoricalConfig

# Fixed, arbitrary namespace for deriving dataset-available event ids —
# never changes; changing it would change every future event's id for a
# file+version pair that was previously deterministic.
_DATASET_EVENT_ID_NAMESPACE = uuid.UUID("6d1f1a2e-8b7a-4b8b-9c3f-9a8f9e6d5c4b")


def _deterministic_event_id(s3_key: str, version: int) -> str:
    """Derive the CloudEvent ``id`` from the true file identity + version —
    not random, and not site_id+category.

    ``site_id`` + ``category`` is a *derived* grouping (category comes from
    parsing the filename via ``extract_category``), not a unique identity:
    two genuinely different S3 objects can extract to the same category and
    would then collide on the same id. ``s3_key`` — the actual S3 object
    key — is always unique per source file, so it's what this must be keyed
    on (see also ``AdlsStore._commit_with_identity_guard``, which guards
    the same non-uniqueness at the storage-path level).

    A redelivered work item that gets as far as re-emitting (e.g. after a
    transient failure that struck after the previous attempt's upload but
    before mark_completed ran) resolves the *same* version again, so this
    produces the *same* id both times. A downstream consumer can then
    dedupe by id instead of silently double-processing the same dataset.
    """
    return str(uuid.uuid5(_DATASET_EVENT_ID_NAMESPACE, f"{s3_key}#v{version}"))


def build_dataset_envelope(
    data: dict,
    config: HistoricalConfig,
    ingestion_id: str,
    traceparent: str,
    ingestion_timestamp: str,
    s3_key: str,
) -> dict:
    """Build a CloudEvents v1.0 envelope for a dataset-available event.

    Produces a ``solar.pvdaq.dataset.available`` envelope conforming to
    ``contracts/dataset-event.json``.  ``mapping_version`` is set to
    ``"unknown"`` per Constitution III fallback (no field mapping at dataset
    level). ``id`` is deterministic — see ``_deterministic_event_id``.

    Args:
        data: Dataset data block dict (site_id, category, file_format,
            storage_path, ingestion_id, source_url, file_size, file_hash).
        config: Historical config providing tenant_id, schema_version.
        ingestion_id: UUID for this ingestion; used as correlation_id for
            lineage tracing.
        traceparent: W3C Trace Context traceparent header value.
        ingestion_timestamp: ISO-8601 timestamp when the file was processed.
        s3_key: The S3 object key this event is for — used only to derive
            the deterministic ``id`` (see ``_deterministic_event_id``); not
            itself included in the envelope body.

    Returns:
        A dict conforming to the dataset CloudEvents envelope contract.
    """
    return {
        "specversion": "1.0",
        "type": "solar.pvdaq.dataset.available",
        "source": "/energy-ingestion-boundary/pvdaq",
        "id": _deterministic_event_id(s3_key, data["version"]),
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
    event_type: str = "solar.pvdaq.dataset.available",
    source: str = "/energy-ingestion-boundary/pvdaq",
) -> dict:
    """Build a CloudEvents v1.0 envelope wrapping a validated PVDAQ record.

    Args:
        record: The validated PVDAQ telemetry record (will be deep-copied).
        config: Application configuration providing tenant_id, schema_version,
            and mapping_version.
        correlation_id: UUID string for invocation tracing.
        traceparent: W3C Trace Context traceparent header value.
        event_type: CloudEvents ``type`` field. Defaults to the currently
            registered ``"solar.pvdaq.dataset.available"`` (``topics.md``).
            ``raw.pvdaq.generation.v1`` is retired per ADR 0002 and must
            never be passed here.
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
