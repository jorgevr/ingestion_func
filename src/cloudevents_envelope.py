"""CloudEvents envelope builder for PVDAQ telemetry records.

Constructs a CloudEvents v1.0 envelope with extension attributes for
tenant, vendor, schema, and tracing metadata.
"""

from __future__ import annotations

import copy
import json as _json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import jsonschema
from jsonschema import Draft202012Validator

from src.config import Config, HistoricalConfig

# Type alias for configs that provide the metadata fields we need
_ConfigLike = Config | HistoricalConfig


class EnvelopeValidationError(Exception):
    """Raised when a constructed dataset-available envelope fails
    validation against the vendored contract (R2.3b F4) — distinct from
    the generic ``jsonschema.ValidationError`` it wraps so
    ``historical_worker`` can give it its own dead-letter reason code
    (``envelope_validation_failure``) rather than lumping it in with every
    other failure under one generic reason."""


# Fixed, arbitrary namespace for deriving dataset-available event ids —
# never changes; changing it would change every future event's id for a
# file+version pair that was previously deterministic.
_DATASET_EVENT_ID_NAMESPACE = uuid.UUID("6d1f1a2e-8b7a-4b8b-9c3f-9a8f9e6d5c4b")

# Vendored copy of the root registry's contracts/dataset-available.v1.json
# (R2.3; ADR 0004) — Contract Owner territory, never hand-edited here;
# re-vendor byte-for-byte from the root on drift, never patch this file
# directly. Loaded once at import time (not lazily on the first emission)
# so a missing or invalid vendored copy fails the whole worker process at
# startup, matching R2.1b's "validate at startup" precedent for the
# work-item schema. `dataschema` is read from the schema's own `$id` rather
# than hardcoded, so it can never drift from the file it's meant to name.
_DATASET_AVAILABLE_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent
    / "schemas"
    / "contracts"
    / "dataset-available.v1.json"
)
_DATASET_AVAILABLE_SCHEMA: dict = _json.loads(
    _DATASET_AVAILABLE_SCHEMA_PATH.read_text(encoding="utf-8")
)
_DATASET_AVAILABLE_DATASCHEMA: str = _DATASET_AVAILABLE_SCHEMA["$id"]
_DATASET_AVAILABLE_VALIDATOR = Draft202012Validator(_DATASET_AVAILABLE_SCHEMA)


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
    correlation_id: str,
    traceparent: str,
    ingestion_timestamp: str,
    s3_key: str,
) -> dict:
    """Build a CloudEvents v1.0 envelope for a dataset-available event.

    Produces a ``solar.pvdaq.dataset.available.v1`` envelope (ADR 0002)
    conforming to the vendored ``contracts/dataset-available.v1.json`` — the
    returned envelope is validated against that same vendored copy before
    this returns, so a shape bug fails loudly at the point of construction,
    never silently in flight to the broker. ``id`` is deterministic — see
    ``_deterministic_event_id``. ``dataschema`` and ``mapping_version`` are
    no longer hardcoded: ``dataschema`` is the vendored schema's own ``$id``
    (see the module-level constant), and ``mapping_version`` comes from
    *config*, matching ``build_envelope``'s existing pattern — ``"unknown"``
    remains the legal explicit sentinel (no field mapping at dataset level)
    but must be configuration-driven, not a literal in this function.

    Args:
        data: Dataset data block dict (site_id, category, file_format,
            storage_path, version, ingestion_id, source_url, file_size,
            file_hash). ``data["ingestion_id"]`` is a fresh identifier for
            *this* ingestion attempt — see the ``correlation_id`` note below.
        config: Historical config providing tenant_id, schema_version,
            mapping_version_pvdaq.
        correlation_id: The originating work item's ``correlation_id`` (ADR
            0003 business key) — the *same* value on every Service Bus
            redelivery of that work item, because it comes from the message
            body, which redelivery does not regenerate. Deliberately **not**
            ``data["ingestion_id"]``: that field is a fresh per-attempt
            identifier (a redelivered attempt gets a new one), and
            conflating the two would make the envelope's correlation_id
            change across redeliveries of the identical message — exactly
            what a downstream consumer needs it to *not* do.
        traceparent: W3C Trace Context traceparent header value.
        ingestion_timestamp: ISO-8601 timestamp when the file was processed.
        s3_key: The S3 object key this event is for — used only to derive
            the deterministic ``id`` (see ``_deterministic_event_id``); not
            itself included in the envelope body.

    Returns:
        A dict conforming to the vendored dataset-available CloudEvents
        envelope contract.

    Raises:
        EnvelopeValidationError: If the constructed envelope does not
            validate against the vendored contract.
    """
    envelope = {
        "specversion": "1.0",
        "type": "solar.pvdaq.dataset.available.v1",
        "source": "/energy-ingestion-boundary/pvdaq",
        "id": _deterministic_event_id(s3_key, data["version"]),
        "time": ingestion_timestamp,
        "datacontenttype": "application/json",
        "dataschema": _DATASET_AVAILABLE_DATASCHEMA,
        "tenant_id": config.tenant_id,
        "source_vendor": "PVDAQ",
        "schema_version": config.schema_version_pvdaq,
        "mapping_version": config.mapping_version_pvdaq,
        "correlation_id": correlation_id,
        "ingestion_timestamp": ingestion_timestamp,
        "traceparent": traceparent,
        "data": data,
    }
    try:
        _DATASET_AVAILABLE_VALIDATOR.validate(envelope)
    except jsonschema.ValidationError as exc:
        raise EnvelopeValidationError(str(exc)) from exc
    return envelope


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
