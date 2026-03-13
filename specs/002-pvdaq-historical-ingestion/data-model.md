# Data Model: PVDAQ Historical Dataset Ingestion

**Feature**: 002-pvdaq-historical-ingestion
**Date**: 2026-03-09 (revised from 2026-03-02)

## Entities

### 1. Site Configuration

Static configuration of the 4 target PVDAQ sites. Stored in environment variables / config.

| Field | Type | Description |
| --- | --- | --- |
| site_id | integer | PVDAQ site identifier (9068, 9069, 2107, 7333) |
| site_name | string | Human-readable name (SR_CO, Simon_Solar_Farm, etc.) |
| s3_prefix | string | S3 path: `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/` |

### 2. File Tracking Entity (Azure Table Storage)

Tracks which CSV files have been discovered, processed, and stored. One row per file. This entity also serves as the dataset metadata store (per clarification session 2026-03-09).

| Field          | Type    | Description                                                                           |
| -------------- | ------- | ------------------------------------------------------------------------------------- |
| PartitionKey   | string  | Site ID as string (e.g., "9068")                                                      |
| RowKey         | string  | SHA-256 hash of the full S3 key, truncated to 64 chars                                |
| S3Key          | string  | Full S3 object key                                                                    |
| FileName       | string  | Basename of the S3 key (e.g., `9068_ac_power_data.csv`)                               |
| Size           | long    | File size in bytes from S3 listing                                                    |
| LastModified   | string  | ISO-8601 timestamp from S3 listing                                                    |
| Status         | string  | One of: `queued`, `processing`, `completed`, `failed`                                 |
| EnqueuedAt     | string  | ISO-8601 timestamp when the work item was dispatched                                  |
| CompletedAt    | string  | ISO-8601 timestamp when the worker finished (nullable)                                |
| CorrelationId  | string  | Correlation ID of the dispatcher run that enqueued this file                          |
| StoragePath    | string  | ADLS Gen2 file path (set on completion)                                               |
| FileHash       | string  | SHA-256 hex digest of the file content, 64 chars (set on completion)                  |
| IngestionId    | string  | UUID identifying this ingestion run (set on completion)                               |
| SourceUrl      | string  | Original S3 download URL (set on completion)                                          |
| IngestionTime  | string  | ISO-8601 timestamp when the file was stored in ADLS (set on completion)               |

**State transitions**: `(new) → queued → processing → completed` or `(new) → queued → processing → failed`

**Uniqueness**: PartitionKey + RowKey. The RowKey hash ensures uniqueness even for long S3 keys.

**Removed field**: `RecordsEmitted` — no longer applicable since this feature does not parse CSV rows.

### 3. Work Item Message (Service Bus Queue)

A message dispatched by the timer-triggered dispatcher for the queue-triggered worker to process. Extended with `category` field (T011).

| Field | Type | Description |
| --- | --- | --- |
| site_id | integer | PVDAQ site identifier |
| s3_key | string | Full S3 object key for the CSV file |
| file_name | string | Basename of the CSV file |
| category | string | Measurement category extracted from filename (e.g., `ac_power`) |
| correlation_id | string | Correlation ID for tracing |
| enqueued_at | string | ISO-8601 timestamp |

### 4. Dataset (logical entity, materialized across tracking table + ADLS)

The primary entity of the revised architecture. Represents a single ingested CSV file stored in ADLS Gen2.

| Field | Type | Storage | Description |
| --- | --- | --- | --- |
| site_id | integer | Tracking table (PK) | PVDAQ site identifier |
| category | string | Derived from filename | Measurement category (e.g., "irradiance") |
| source_url | string | Tracking table | Original S3 download URL |
| storage_path | string | Tracking table + ADLS | ADLS Gen2 deterministic path |
| file_size | integer | Tracking table | File size in bytes |
| file_hash | string | Tracking table | SHA-256 hex digest (64 chars) |
| ingestion_id | string | Tracking table | UUID for this ingestion |
| ingestion_time | string | Tracking table | ISO-8601 when stored in ADLS |

### 5. Dataset Event (CloudEvents envelope, emitted to Service Bus)

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

| Field         | Type    | Description                                   |
| ------------- | ------- | --------------------------------------------- |
| site_id       | integer | PVDAQ site identifier                         |
| category      | string  | Measurement category                          |
| file_format   | string  | Always `"csv"`                                |
| storage_path  | string  | ADLS Gen2 path                                |
| ingestion_id  | string  | UUID                                          |
| source_url    | string  | Original S3 URL                               |
| file_size     | integer | File size in bytes                            |
| file_hash     | string  | SHA-256 hex digest                            |

## Relationships

```text
Site Configuration (4 sites)
  └── has many → CSV Files (discovered via S3 listing)
       └── tracked by → File Tracking Entity (Table Storage)
       └── dispatched as → Work Item Message (Service Bus Queue)
       └── stored as → ADLS Gen2 file (raw container)
       └── emitted as → Dataset Event (Service Bus Topic)
```

## What Was Removed

The following entities from the previous data model are no longer part of feature 002:

- **Telemetry Record** (in-memory CSV row) — moved to downstream processing layer
- **Idempotency Entry** (per-row dedup) — replaced by file-level tracking in the File Tracking Entity
- **CloudEvents Envelope** (per-row) — replaced by dataset-level Dataset Event
