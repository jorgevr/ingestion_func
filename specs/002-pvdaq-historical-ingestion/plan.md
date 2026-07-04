# Implementation Plan: PVDAQ Historical Dataset Ingestion

**Branch**: `002-pvdaq-historical-ingestion` | **Date**: 2026-03-19 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/002-pvdaq-historical-ingestion/spec.md`

## Summary

Ingest all historical PVDAQ photovoltaic CSV files (4 sites, up to 870 MB per file) from the OEDI
public S3 bucket into ADLS Gen2 (bronze layer), register dataset metadata in Azure Table Storage,
and emit a `solar.pvdaq.dataset.available` CloudEvent per file. Uses a fan-out pattern:
timer-triggered dispatcher discovers and enqueues files; queue-triggered worker downloads, stores,
and emits. Most modules are already implemented. The primary outstanding work is fixing the ADLS
path (Constitution VIII gate failure), threading `last_modified` through the work item, adding
work-item schema validation, and wiring local emulation switching.

## Technical Context

**Language/Version**: Python 3.11+, Azure Functions v4 Isolated Worker (v2 programming model)
**Primary Dependencies**: azure-functions, azure-servicebus, azure-data-tables, azure-identity,
azure-storage-file-datalake, httpx, jsonschema — all present in `requirements.txt`
**Storage**: Azure Table Storage (`PvdaqFileTracking`) + ADLS Gen2 bronze container (`raw`)
**Testing**: pytest + pytest-asyncio; unit in `tests/unit/`, integration in `tests/integration/`,
contract in `tests/contract/`
**Target Platform**: Azure Functions v4 (Linux), Azurite for local development
**Performance Goals**: Memory ≤ 4 MiB per file (streaming, chunked); no per-run throughput target
**Constraints**: Function timeout 01:10:00 (host.json); each file processed independently within
that window; S3 retries default 3 (MAX_RETRIES in http_retry.py)
**Scale/Scope**: 4 sites × ~10 CSV files each = ~40 total files; files 7 MB–870 MB

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
| --------- | ------ | ----- |
| I — Function Isolation | ✅ PASS | `historical_dispatcher` and `historical_worker` are independent functions with own error handling. Failure in worker does not cascade to dispatcher. |
| II — Schema Validation at Boundary | ⚠️ PARTIAL | Work-item queue message is not validated against `work-item-message.json` in `historical_worker`. All other inbound payloads (CloudEvents envelope) are validated via contract tests. **Must fix before merge.** |
| III — Metadata Enrichment | ✅ PASS | `build_dataset_envelope` adds `source_vendor`, `ingestion_timestamp`, `schema_version`, `mapping_version` (sentinel "unknown"), `correlation_id`, `traceparent`. |
| IV — Managed Identity | ✅ PASS | `DefaultAzureCredential` used for all Azure SDK clients. Local dev uses Azurite connection string via `AzureWebJobsStorage`. `ADLS_ACCOUNT_URL` local override required (see gap below). |
| V — Structured Observability | ✅ PASS | `DatasetIngestionStats` + `emit_dataset_metrics` covers all FR-011 fields. JSON-structured logging via `create_logger`. |
| VI — Idempotency | ✅ PASS | `FileTrackingStore.mark_queued` called before `send_queue_message` (write-before-emit). Worker marks processing → completed/failed. |
| VII — Event Emission Rules | ✅ PASS | `solar.pvdaq.dataset.available` registered in `topics.md`. CloudEvents v1.0 envelope. Dead-letter routing on failure. |
| VIII — Raw Dataset Storage | ❌ **GATE FAILURE** | `function_app.py:341` builds path `pvdaq/site_id={site_id}/category={category}/{file_name}`. Spec and Constitution VIII require `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}`. Missing `raw/` prefix and `year/month` partitioning. **Blocks merge until fixed.** |
| Constitution: host.json concurrency | ✅ PASS | `maxConcurrentCalls: 1` explicitly set for Service Bus queue trigger. `functionTimeout: 01:10:00` accommodates large file downloads. |

### Complexity Tracking

**Constitution VIII — ADLS path**: current path omits the `raw/` prefix and uses `category` instead of `year/month`. Fix requires adding `last_modified` to the work-item contract so the worker can derive UTC `year`/`month` without an extra S3 call. Keeping a category partition was rejected because it diverges from the standardised bronze path convention shared with feature 001.

## Project Structure

### Documentation (this feature)

```text
specs/002-pvdaq-historical-ingestion/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/
│   ├── dataset-event.json         # CloudEvents envelope schema ✅
│   ├── file-tracking-entity.json  # Table Storage entity schema ✅
│   └── work-item-message.json     # Queue message schema (needs last_modified field)
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code

```text
function_app.py              # Azure Functions entry point — dispatcher + worker
src/
├── config.py                # HistoricalConfig dataclass + load_historical_config()
├── adls_store.py            # AdlsStore: streaming upload with SHA-256
├── file_tracking_store.py   # FileTrackingStore: Azure Table Storage CRUD
├── oedi_historical_client.py # OediHistoricalClient: S3 list + stream
├── service_bus_emitter.py   # ServiceBusEmitter: topic/queue/dead-letter send
├── cloudevents_envelope.py  # build_dataset_envelope()
├── observability.py         # DatasetIngestionStats, emit_dataset_metrics
├── http_retry.py            # get_with_retry: MAX_RETRIES=3, exponential backoff
└── schema_validator.py      # JSON Schema validation (reuse for work-item validation)

tests/
├── contract/
│   ├── test_cloudevents_envelope.py   # CloudEvents envelope shape
│   └── test_historical_envelope.py    # dataset-event.json contract
├── integration/
│   ├── test_historical_pipeline.py    # Dispatcher + worker integration
│   └── test_dead_letter.py            # Dead-letter routing
└── unit/
    ├── test_oedi_historical_client.py # list_csv_files pagination, extract_category
    └── test_file_tracking_store.py    # CRUD, LastModified-only change detection
```

## Phase 0: Research Summary

See [research.md](research.md) for full findings.

**All NEEDS CLARIFICATION items resolved:**

| Item | Decision |
|------|----------|
| ADLS path format | `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}` — from spec FR-004 and Constitution VIII. `year`/`month` derived in UTC from S3 `LastModified`. |
| Work-item contract gap | Add `last_modified` field to `work-item-message.json` so worker can derive year/month without a second S3 call. |
| Local ADLS emulation | Azurite Blob (port 10000) using same `azure-storage-file-datalake` SDK. Switch via `ADLS_ACCOUNT_URL=http://127.0.0.1:10000/devstoreaccount1` when `STORAGE_EMULATOR=true`. |
| Local Service Bus emulation | Azure Service Bus emulator (Docker). Same queue names (`raw-energy-events`, `pvdaq-historical-work`, `pvdaq-dead-letter`) as production. `ServiceBusConnection` uses `UseDevelopmentEmulator=true`. Azurite Queue is NOT used for Service Bus. |
| File change detection | Compare S3 `LastModified` (from current listing) against `LastModified` stored in tracking entity. Size check is a secondary guard and can remain. |
| Schema validation for queue messages | Reuse existing `SchemaValidator` from `schema_validator.py` with `work-item-message.json`. Validate at top of `historical_worker` before any processing. |
| S3 listing pagination | Already implemented — `OediHistoricalClient.list_csv_files` loops on `IsTruncated` / `NextContinuationToken`. |
| Retry default | `MAX_RETRIES = 3` already set in `http_retry.py`. |
| Timezone | All year/month derivation uses UTC (`datetime.fromisoformat(last_modified).astimezone(timezone.utc)`). |
| Concurrency | `maxConcurrentCalls: 1` in `host.json` — host-level only, no application-level semaphore. |

## Phase 1: Design

See [data-model.md](data-model.md) for entity schemas and state machine.
See [contracts/](contracts/) for JSON Schema contracts.
See [quickstart.md](quickstart.md) for local dev setup.

### Key Design Decisions

#### D-001: ADLS Path Construction (bronze layer convention)

The worker builds a versioned, append-only path following the bronze folder convention.
`ingestion_date` is the UTC calendar date of ingestion (NOT the S3 LastModified date).
`dataset` is `{site_id}_{category}` (e.g. `9068_ac_power`).

```python
from datetime import datetime, timezone

def _adls_path(site_id: int, category: str, ingestion_date: str, version: int) -> str:
    dataset = f"{site_id}_{category}"
    return (
        f"source=pvdaq"
        f"/dataset={dataset}"
        f"/ingestion_date={ingestion_date}"
        f"/{dataset}_v{version}.csv"
    )

def _metadata_path(site_id: int, category: str, ingestion_date: str) -> str:
    dataset = f"{site_id}_{category}"
    return (
        f"source=pvdaq"
        f"/dataset={dataset}"
        f"/ingestion_date={ingestion_date}"
        f"/metadata.json"
    )
```

`last_modified` is still threaded through the work item for **change detection** (FR-012),
not for path construction.

#### D-002: Work-Item Contract Extension

`work-item-message.json` gains an optional `last_modified` field:

- Type: `string`, format `date-time`
- Populated by dispatcher from S3 listing `last_modified`
- Used by worker for change detection; falls back gracefully if absent

#### D-003: Work-Item Schema Validation

At the top of `historical_worker`, before any state mutation:

```python
from src.schema_validator import SchemaValidator
validator = SchemaValidator()
errors = validator.validate(work_item, "work-item-message.json")
if errors:
    # dead-letter and return — do not mark_processing
```

Schema file location: `specs/002-pvdaq-historical-ingestion/contracts/work-item-message.json`.
`SchemaValidator` must be extended to resolve schemas from this path.

#### D-004: Local Emulation Switching

When `STORAGE_EMULATOR=true` (or `AzureWebJobsStorage == "UseDevelopmentStorage=true"`):

- ADLS: set `ADLS_ACCOUNT_URL=http://127.0.0.1:10000/devstoreaccount1` in `local.settings.json`
  (no code change needed — `AdlsStore` already accepts `account_url` from config)
- Service Bus (local + prod): `ServiceBusEmitter` uses `azure-servicebus` for all environments.
  Locally the Service Bus emulator (Docker, `UseDevelopmentEmulator=true`) is used; in production
  `ServiceBusConnection__fullyQualifiedNamespace` points to `jorgevr.servicebus.windows.net`.
  Azurite Queue (`azure-storage-queue`) is NOT used. `STORAGE_EMULATOR` controls only ADLS/Table emulation.

#### D-005: Integration Test Path Assertion Update

`tests/integration/test_historical_pipeline.py` must be updated to assert the new versioned
bronze path `source=pvdaq/dataset=9068_ac_power/ingestion_date=YYYY-MM-DD/9068_ac_power_v1.csv`
and verify that `metadata.json` is written alongside it.

#### D-006: Versioning Strategy (append-only, never overwrite)

Before writing, the worker determines the next version number by querying `PvdaqFileTracking`
for all entities with the same `PartitionKey` (site_id) and `S3Key`, then computing
`max(Version) + 1`. The result is used in both the ADLS path and the RowKey
(`SHA256(s3_key)_v{version}`). This means re-ingestion creates a new entity, leaving previous
entities untouched — fully append-only.

```python
async def _next_version(tracker, site_id: int, s3_key: str) -> int:
    """Query tracking table for max version of this dataset, return next version."""
    existing = await tracker.get_versions(site_id, s3_key)
    return max(existing, default=0) + 1
```

`FileTrackingStore` gains a `get_versions(site_id, s3_key) -> list[int]` method that queries
all entities matching the partition key and S3 key, returning their `Version` values.

#### D-007: Metadata File Writing

After the CSV stream upload completes, the worker constructs a `metadata.json` conforming to
`contracts/metadata-file.json` and writes it via `AdlsStore.write_json(path, data)` — a simple
single-chunk write (no streaming needed since metadata is always small, < 2 KB).

The file is structured in 7 blocks. Bronze populates what it can **without opening the CSV**:

```python
metadata = {
    "dataset": {
        "dataset_id": f"{site_id}_{category}",
        "dataset_type": "time_series",
        "version": version,
        "schema_version": "unknown",
        "tags": ["pvdaq", "solar"],
    },
    "source": {
        "source": "pvdaq",
        "source_type": "s3_public",
        "endpoint": f"pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/",
        "provider": "NREL",
        "region": "us-east-1",
    },
    "ingestion": {
        "ingestion_time": ingestion_time,
        "ingestion_id": ingestion_id,
        "batch_id": correlation_id,          # from dispatcher
        "pipeline": "energy-ingestion-boundary-v1",
        "trigger_type": "rerun" if version > 1 else "scheduled",
        "retry_count": 0,
        "source_file_name": file_name,
        "file_size_bytes": file_size,        # from S3 listing
        "checksum": file_hash,              # SHA-256 from stream_upload
        "ingestion_latency_seconds": round(elapsed, 1),
        "status": "success",
    },
    "event_time": {
        "event_time_start": None,           # null — silver layer fills
        "event_time_end": None,             # null — silver layer fills
        "expected_frequency_seconds": 300,  # PVDAQ 5-min intervals
        "expected_records": None,           # null — depends on event_time
    },
    "data_profile": {
        "row_count": row_count,             # newline count from stream_upload
        "null_percentage": None,
        "duplicate_rows": None,
        "min_timestamp": None,
        "max_timestamp": None,
        "schema_detected": None,
        "corrupted_rows": None,
    },
    "quality_hint": {
        "basic_quality_score": None,
        "schema_valid": None,
        "time_continuity_suspected_gap": None,
        "notes": [],
    },
    "lineage": {
        "parent_dataset_version": version - 1 if version > 1 else None,
        "rerun_of": f"{site_id}_{category}_v{version - 1}" if version > 1 else None,
        "related_incident_id": None,
    },
}
```

`row_count` is computed by counting `\n` bytes in the same chunking loop that uploads to ADLS —
zero extra I/O, no CSV column parsing. `stream_upload` returns a 3-tuple
`(bytes_written, sha256_hex, newline_count)` after this change.

### Re-evaluation of Constitution Check (post-design)

| Principle | Status |
|-----------|--------|
| I — Function Isolation | ✅ |
| II — Schema Validation | ✅ D-003 adds work-item validation |
| III — Metadata Enrichment | ✅ |
| IV — Managed Identity | ✅ D-004 clarifies local URL override |
| V — Structured Observability | ✅ |
| VI — Idempotency | ✅ |
| VII — Event Emission Rules | ✅ |
| VIII — Raw Dataset Storage | ✅ D-001 fixes path to `raw/pvdaq/site_id=.../year=.../month=.../{file}` |
