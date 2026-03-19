# Data Model: PVDAQ Historical Dataset Ingestion

**Feature**: 002-pvdaq-historical-ingestion
**Date**: 2026-03-19 (revised from 2026-03-09)

## Entities

### 1. Site Configuration

Static configuration of the 4 target PVDAQ sites. Stored in environment variables / config.

| Field | Type | Description |
| --- | --- | --- |
| site_id | integer | PVDAQ site identifier (9068, 9069, 2107, 7333) |
| site_name | string | Human-readable name (SR_CO, Simon_Solar_Farm, etc.) |
| s3_prefix | string | S3 path: `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/` |

### 2. File Tracking Entity (Azure Table Storage)

Tracks which CSV files have been discovered, processed, and stored. One row per **ingestion version** — a file re-ingested at a new version creates a new entity rather than updating the existing one (append-only). This entity also serves as the dataset metadata store.

| Field          | Type    | Description                                                                           |
| -------------- | ------- | ------------------------------------------------------------------------------------- |
| PartitionKey   | string  | Site ID as string (e.g., "9068")                                                      |
| RowKey         | string  | `SHA256(s3_key)_v{version}` — unique per ingestion version                            |
| S3Key          | string  | Full S3 object key                                                                    |
| FileName       | string  | Basename of the S3 key (e.g., `9068_ac_power_data.csv`)                               |
| Category       | string  | Measurement category (e.g., `ac_power`)                                               |
| Version        | integer | Ingestion version number (1 = first, increments on each re-ingestion)                 |
| Size           | long    | File size in bytes from S3 listing                                                    |
| LastModified   | string  | ISO-8601 S3 LastModified timestamp (used for change detection)                        |
| Status         | string  | One of: `queued`, `processing`, `completed`, `failed`                                 |
| EnqueuedAt     | string  | ISO-8601 timestamp when the work item was dispatched                                  |
| CompletedAt    | string  | ISO-8601 timestamp when the worker finished (nullable)                                |
| CorrelationId  | string  | Correlation ID of the dispatcher run that enqueued this file                          |
| StoragePath    | string  | ADLS bronze path (see Storage Layout for format; set on completion)                   |
| FileHash       | string  | SHA-256 hex digest of file content, 64 chars (set on completion)                      |
| IngestionId    | string  | UUID identifying this ingestion run (set on completion)                               |
| SourceUrl      | string  | Original S3 download URL (set on completion)                                          |
| IngestionTime  | string  | ISO-8601 timestamp when file was stored in ADLS (set on completion)                   |
| RowCount       | integer | Newline count during streaming upload (set on completion)                              |
| MetadataPath   | string  | ADLS path of the accompanying `metadata.json` (set on completion)                     |

**State transitions**: `(new) → queued → processing → completed` or `(new) → queued → processing → failed`

**Uniqueness**: PartitionKey + RowKey. RowKey includes the version suffix so each version of the same file has its own entity. Previous version entities are never deleted (append-only).

**Version resolution**: Before writing, the worker queries `PvdaqFileTracking` for all entities with matching `PartitionKey` and `S3Key` to find the current maximum `Version`. The next version is `max + 1`; first ingestion is `1`.

### 3. Work Item Message (Service Bus Queue)

A message dispatched by the timer-triggered dispatcher for the queue-triggered worker to process.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| site_id | integer | ✅ | PVDAQ site identifier |
| s3_key | string | ✅ | Full S3 object key for the CSV file |
| file_name | string | ✅ | Basename of the CSV file |
| category | string | ✅ | Measurement category extracted from filename (e.g., `ac_power`) |
| correlation_id | string | ✅ | Correlation ID for tracing |
| enqueued_at | string | ✅ | ISO-8601 timestamp |
| last_modified | string | optional | ISO-8601 S3 LastModified timestamp — used by the worker for change detection and version resolution. Falls back gracefully if absent. |

### 4. Dataset (logical entity, materialized across tracking table + ADLS)

The primary entity of the revised architecture. Represents a single ingested version of a CSV file stored in ADLS Gen2.

| Field | Type | Storage | Description |
| --- | --- | --- | --- |
| site_id | integer | Tracking table (PK) | PVDAQ site identifier |
| category | string | Derived from filename | Measurement category (e.g., "irradiance") |
| version | integer | Tracking table | Ingestion version number |
| source_url | string | Tracking table | Original S3 download URL |
| storage_path | string | Tracking table + ADLS | ADLS Gen2 path: `source=pvdaq/dataset={dataset}/ingestion_date={date}/{dataset}_v{version}.csv` |
| file_size | integer | Tracking table | File size in bytes |
| file_hash | string | Tracking table | SHA-256 hex digest (64 chars) |
| row_count | integer | Tracking table + metadata.json | Newline count during streaming |
| ingestion_id | string | Tracking table | UUID for this ingestion |
| ingestion_time | string | Tracking table + metadata.json | ISO-8601 when stored in ADLS |

### 5. Metadata File (written to ADLS alongside each CSV)

A `metadata.json` file written at `source=pvdaq/dataset={dataset}/ingestion_date={date}/metadata.json` alongside the CSV for every successfully stored dataset. Full schema in `contracts/metadata-file.json`.

The file is structured in 7 blocks. Each block answers a distinct question:

| Block | Question | Bronze populates? |
| --- | --- | --- |
| `dataset` | WHAT is this file? | ✅ fully |
| `source` | WHERE did it come from? | ✅ fully |
| `ingestion` | HOW did it arrive? | ✅ fully |
| `event_time` | WHAT time does the data represent? | ⚠️ range fields null (need CSV parsing) |
| `data_profile` | Lightweight data signals | ⚠️ only `row_count` (newline count) |
| `quality_hint` | Early quality signals | ❌ all null (require profiling) |
| `lineage` | Audit trail and rerun tracking | ✅ version-based fields |

**Populated at bronze** (no CSV column parsing required):

| Field | Location | Value |
| --- | --- | --- |
| dataset_id | dataset | `"{site_id}_{category}"` |
| dataset_type | dataset | `"time_series"` |
| version | dataset | incremental integer |
| schema_version | dataset | `"unknown"` |
| tags | dataset | `["pvdaq", "solar"]` |
| source | source | `"pvdaq"` |
| source_type | source | `"s3_public"` |
| endpoint | source | S3 prefix path |
| provider | source | `"NREL"` |
| region | source | `"us-east-1"` |
| ingestion_time | ingestion | UTC timestamp at write |
| ingestion_id | ingestion | UUID (shared with CloudEvent) |
| batch_id | ingestion | dispatcher correlation_id |
| pipeline | ingestion | `"energy-ingestion-boundary-v1"` |
| trigger_type | ingestion | `"scheduled"` or `"rerun"` (version > 1) |
| retry_count | ingestion | S3 retry count (default 0) |
| source_file_name | ingestion | original S3 basename |
| file_size_bytes | ingestion | size from S3 listing |
| checksum | ingestion | SHA-256 computed during streaming |
| ingestion_latency_seconds | ingestion | elapsed seconds for stream write |
| status | ingestion | `"success"` |
| expected_frequency_seconds | event_time | `300` (5-min PVDAQ measurement intervals) |
| row_count | data_profile | newline count during streaming |
| notes | quality_hint | `[]` (populated if anomaly detected) |
| parent_dataset_version | lineage | version - 1, or null if first |
| rerun_of | lineage | `"{dataset}_v{n-1}"` or null |
| related_incident_id | lineage | null for scheduled runs |

**Null at bronze** (require CSV column parsing — populated by silver layer, feature 003+):
`event_time_start`, `event_time_end`, `expected_records`, `null_percentage`, `duplicate_rows`, `min_timestamp`, `max_timestamp`, `schema_detected`, `corrupted_rows`, `basic_quality_score`, `schema_valid`, `time_continuity_suspected_gap`

**Example (first ingestion)**:

```json
{
  "dataset": {
    "dataset_id": "9068_ac_power",
    "dataset_type": "time_series",
    "version": 1,
    "schema_version": "unknown",
    "tags": ["pvdaq", "solar"]
  },
  "source": {
    "source": "pvdaq",
    "source_type": "s3_public",
    "endpoint": "pvdaq/2023-solar-data-prize/9068_OEDI/data/",
    "provider": "NREL",
    "region": "us-east-1"
  },
  "ingestion": {
    "ingestion_time": "2024-01-15T08:32:00Z",
    "ingestion_id": "3f4a1b2c-...",
    "batch_id": "a1b2c3d4-...",
    "pipeline": "energy-ingestion-boundary-v1",
    "trigger_type": "scheduled",
    "retry_count": 0,
    "source_file_name": "9068_ac_power_data.csv",
    "file_size_bytes": 65000000,
    "checksum": "a3f1...64hexchars",
    "ingestion_latency_seconds": 47.3,
    "status": "success"
  },
  "event_time": {
    "event_time_start": null,
    "event_time_end": null,
    "expected_frequency_seconds": 300,
    "expected_records": null
  },
  "data_profile": {
    "row_count": 105121,
    "null_percentage": null,
    "duplicate_rows": null,
    "min_timestamp": null,
    "max_timestamp": null,
    "schema_detected": null,
    "corrupted_rows": null
  },
  "quality_hint": {
    "basic_quality_score": null,
    "schema_valid": null,
    "time_continuity_suspected_gap": null,
    "notes": []
  },
  "lineage": {
    "parent_dataset_version": null,
    "rerun_of": null,
    "related_incident_id": null
  }
}
```

### 6. Dataset Event (CloudEvents envelope, emitted to Service Bus)

A CloudEvent of type `solar.pvdaq.dataset.available` emitted per successfully stored dataset.

| Field               | Type   | Description                                    |
| ------------------- | ------ | ---------------------------------------------- |
| specversion         | string | `"1.0"`                                        |
| type                | string | `"solar.pvdaq.dataset.available"`              |
| source              | string | `"/energy-ingestion-boundary/pvdaq"`           |
| id                  | string | UUID v4                                        |
| time                | string | ISO-8601 emission timestamp                    |
| datacontenttype     | string | `"application/json"`                           |
| tenant_id           | string | From config (default: `"default"`)             |
| source_vendor       | string | `"PVDAQ"`                                      |
| schema_version      | string | `"v1"`                                         |
| correlation_id      | string | Same as ingestion_id for lineage tracing       |
| ingestion_timestamp | string | ISO-8601                                       |
| traceparent         | string | W3C Trace Context                              |
| data                | object | Dataset metadata (see below)                   |

**data block**:

| Field         | Type    | Description                                                                          |
| ------------- | ------- | ------------------------------------------------------------------------------------ |
| site_id       | integer | PVDAQ site identifier                                                                |
| category      | string  | Measurement category                                                                 |
| file_format   | string  | Always `"csv"`                                                                       |
| storage_path  | string  | ADLS Gen2 path (`source=pvdaq/dataset={d}/ingestion_date={date}/{d}_v{n}.csv`)       |
| version       | integer | Ingestion version number                                                             |
| ingestion_id  | string  | UUID                                                                                 |
| source_url    | string  | Original S3 URL                                                                      |
| file_size     | integer | File size in bytes                                                                   |
| file_hash     | string  | SHA-256 hex digest                                                                   |

## Relationships

```text
Site Configuration (4 sites)
  └── has many → CSV Files (discovered via S3 listing)
       └── tracked by → File Tracking Entity (Table Storage, one per ingestion version)
       └── dispatched as → Work Item Message (Service Bus Queue)
       └── stored as → ADLS Gen2 versioned CSV + metadata.json (bronze container)
       └── emitted as → Dataset Event (Service Bus Topic)
```

## What Was Removed

The following entities from the previous data model are no longer part of feature 002:

- **Telemetry Record** (in-memory CSV row) — moved to downstream processing layer
- **Idempotency Entry** (per-row dedup) — replaced by file-level versioned tracking
- **CloudEvents Envelope** (per-row) — replaced by dataset-level Dataset Event
