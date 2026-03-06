# Data Model: PVDAQ Historical Data Ingestion

**Feature**: 002-pvdaq-historical-ingestion
**Date**: 2026-03-02

## Entities

### 1. Site Configuration

Static configuration of the 4 target PVDAQ sites. Stored in environment variables / config.

| Field | Type | Description |
| --- | --- | --- |
| site_id | integer | PVDAQ site identifier (9068, 9069, 2107, 7333) |
| site_name | string | Human-readable name (SR_CO, Simon_Solar_Farm, etc.) |
| s3_prefix | string | S3 path: `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/` |

### 2. File Tracking Entity (Azure Table Storage)

Tracks which CSV files have been discovered and processed. One row per file.

| Field | Type | Description |
| --- | --- | --- |
| PartitionKey | string | Site ID as string (e.g., "9068") |
| RowKey | string | SHA-256 hash of the full S3 key, truncated to 64 chars |
| S3Key | string | Full S3 object key (e.g., `pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_data.csv`) |
| FileName | string | Basename of the S3 key (e.g., `9068_ac_power_data.csv`) |
| Size | long | File size in bytes from S3 listing |
| LastModified | string | ISO-8601 timestamp from S3 listing |
| Status | string | One of: `queued`, `processing`, `completed`, `failed` |
| EnqueuedAt | string | ISO-8601 timestamp when the work item was dispatched |
| CompletedAt | string | ISO-8601 timestamp when the worker finished (nullable) |
| RecordsEmitted | integer | Number of records successfully emitted from this file |
| CorrelationId | string | Correlation ID of the dispatcher run that enqueued this file |

**State transitions**: `(new) → queued → processing → completed` or `(new) → queued → processing → failed`

**Uniqueness**: PartitionKey + RowKey. The RowKey hash ensures uniqueness even for long S3 keys.

### 3. Work Item Message (Service Bus Queue)

A message dispatched by the timer-triggered dispatcher for the queue-triggered worker to process.

| Field | Type | Description |
| --- | --- | --- |
| site_id | integer | PVDAQ site identifier |
| s3_key | string | Full S3 object key for the CSV file |
| file_name | string | Basename of the CSV file |
| correlation_id | string | Correlation ID for tracing |
| enqueued_at | string | ISO-8601 timestamp |

### 4. Telemetry Record (in-memory, per CSV row)

A single normalized row from a CSV file, ready for schema validation and emission.

| Field | Type | Source | Description |
| --- | --- | --- | --- |
| SiteID | integer | Injected from S3 path | PVDAQ site identifier |
| measdatetime | string | `measured_on` column | Measurement timestamp |
| *(measurement fields)* | number or string | Remaining CSV columns | Sensor readings with suffixes stripped |

**Note**: Unlike feature 001 CSVs, the 2023-solar-data-prize CSVs do NOT contain a `system_id` column. The `SiteID` is extracted from the S3 folder path (`{site_id}_OEDI`) and injected during normalization.

### 5. Idempotency Entry (Azure Table Storage)

Reuses the existing `IdempotencyStore` from feature 001 with an extended key format.

| Field | Type | Description |
| --- | --- | --- |
| PartitionKey | string | Date portion of measdatetime: `YYYY-MM-DD` |
| RowKey | string | `{site_id}_{filename}_{measdatetime}` — includes filename as category discriminator |
| Status | string | `pending` or `completed` |
| CorrelationId | string | Invocation correlation ID |
| CreatedAt | string | ISO-8601 timestamp |
| CompletedAt | string | ISO-8601 timestamp (nullable) |

**Key composition**: Uses filename (basename without `.csv`) as the category component, since file naming conventions vary per site and a generic "category" extraction is fragile.

### 6. CloudEvents Envelope (emitted message)

Same structure as feature 001 with a different `type` field.

| Field | Type | Description |
| --- | --- | --- |
| specversion | string | `"1.0"` |
| type | string | `"raw.pvdaq.historical.v1"` |
| source | string | `"/energy-ingestion-boundary/pvdaq-historical"` |
| id | string | UUID v4 |
| time | string | ISO-8601 emission timestamp |
| datacontenttype | string | `"application/json"` |
| tenant_id | string | From config (default: `"research"`) |
| source_vendor | string | `"PVDAQ"` |
| schema_version | string | From config (e.g., `"v1"`) |
| mapping_version | string | From config |
| correlation_id | string | Invocation correlation ID |
| ingestion_timestamp | string | ISO-8601 |
| traceparent | string | W3C Trace Context |
| data | object | The normalized telemetry record |

## Relationships

```text
Site Configuration (4 sites)
  └── has many → CSV Files (discovered via S3 listing)
       └── tracked by → File Tracking Entity (Table Storage)
       └── dispatched as → Work Item Message (Service Bus Queue)
       └── contains many → Telemetry Records (CSV rows)
            └── checked against → Idempotency Entry (Table Storage)
            └── emitted as → CloudEvents Envelope (Service Bus Topic)
```
