# Feature Specification: PVDAQ Historical Dataset Ingestion

**Feature Branch**: `002-pvdaq-historical-ingestion`
**Created**: 2026-03-02
**Updated**: 2026-03-18
**Status**: Draft
**Input**: User description: "Discover, download, and store historical photovoltaic CSV files from the OEDI Data Lake for 4 specific PVDAQ sites into ADLS Gen2, registering metadata and emitting a dataset-level CloudEvent per file."

## Clarifications

### Session 2026-03-02

- Q: What is the relationship between this feature (002) and the existing feature 001 pipeline? → A: New separate function in the same function app — reuses shared modules (emitter, config patterns), independent trigger.
- Q: How should the system handle Azure Function timeout limits given CSV files up to 870 MB? → A: Fan-out pattern — timer-triggered function lists files and dispatches one message per file to a queue; a queue-triggered function processes each file independently.

### Session 2026-03-09 — Architecture Revision

- Q: What is the event granularity? → A: **One CloudEvent per dataset (file)**, not per row. Row-level parsing, validation, schema normalization, and per-row emission move to a downstream processing layer (feature 003+). This feature is now **dataset ingestion only**: discover → download → store → register → emit dataset event.
- Q: How should idempotency work? → A: Per-file idempotency keyed on `site_id + category + file_name`. Much simpler and cheaper than per-row tracking.
- Q: Where are files stored? → A: Azure Data Lake Storage Gen2 raw container (or Azurite Blob locally) using a deterministic path: `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}.csv`.
- Q: Where is dataset metadata stored? → A: Extend the existing `PvdaqFileTracking` Azure Table with new columns (storage_path, file_hash, ingestion_id) rather than creating a separate store. Keeps file tracking and metadata in one table.
- Q: Does feature 001 continue with per-row processing? → A: Feature 001 has also migrated to the dataset-level pattern (spec updated 2026-03-18). Both 001 and 002 now share the same bronze storage pattern.
- Q: What hash algorithm for file_hash? → A: SHA-256 — consistent with the existing `IdempotencyStore._row_key()` which already uses SHA-256, and provides strong integrity guarantees for large files.

### Session 2026-03-19

- Q: What is the default maximum retry count for S3 HTTP retries (FR-009)? → A: 3 retries.
- Q: What timezone is used when deriving `year` and `month` from the S3 LastModified date for the ADLS path? → A: UTC.
- Q: Should the worker function have an application-level concurrency cap on parallel downloads? → A: No application-level cap; rely on host-level `maxConcurrentCalls` in `host.json`. Per-file idempotency store prevents duplicate work if races occur.
- Q: Which signal determines that an existing file has changed and should be re-ingested? → A: S3 `LastModified` timestamp only, compared against the stored `ingestion_time` in the tracking table.
- Q: Should S3 prefix listing be paginated? → A: Yes — paginate using `NextContinuationToken` until `IsTruncated` is false, to handle sites with >1000 files.

### Session 2026-03-19 (continued)

- Q: What is the ADLS container name — `bronze` (spec) or `raw` (`local.settings.json` + tests)? → A: Container `bronze`; path within the container must follow the bronze layer folder convention (see Session 2026-03-19 bronze conventions below). `local.settings.json` and integration tests must use `ADLS_CONTAINER_NAME=bronze`.
- Q: For incremental file detection, which comparison determines re-ingestion: current S3 `LastModified` vs stored S3 `LastModified`, or vs stored `ingestion_time`? → A: Compare current S3 `LastModified` against the stored S3 `LastModified` in the tracking entity. This matches the implementation and avoids clock-skew issues between S3 and Azure.

### Session 2026-03-19 — Bronze Layer Conventions

- Q: What folder structure must the bronze container follow? → A: `source=pvdaq/dataset={site_id}_{category}/ingestion_date=YYYY-MM-DD/{dataset}_v{version}.csv` plus a `metadata.json` alongside each CSV. No `raw/` prefix — the container itself is the bronze layer. `ingestion_date` is the UTC date of ingestion, not the S3 LastModified date.
- Q: How should files be named? → A: `{dataset}_v{version}.csv` where `{dataset}` is `{site_id}_{category}` (e.g. `9068_ac_power`) and `{version}` is an incrementing integer starting at 1. Example: `9068_ac_power_v1.csv`.
- Q: Should each ingestion create a new file version rather than overwrite? → A: Yes — append-only. Every ingestion of the same logical dataset creates a new version (v1, v2, …). Previous versions are never overwritten or deleted. Re-ingestion triggered by a changed S3 `LastModified` creates the next version.
- Q: What metadata must be written alongside each CSV? → A: A `metadata.json` file at the same path prefix, containing: `dataset_id`, `source`, `version`, `ingestion_time`, `checksum` (SHA-256), `status: "raw"`. `row_count` is computed by counting newline characters during streaming (no semantic CSV parsing). `event_time_start` and `event_time_end` are set to `null` at the bronze layer — they are computed by the silver layer which knows the schema.
- Q: How is the version number determined? → A: Query the `PvdaqFileTracking` table for the highest `Version` value stored for the given `(site_id, category)` combination. The next version is `max_version + 1`; first ingestion is version 1.

### Session 2026-03-18 — Local Emulation

- Q: How should ADLS Gen2 be emulated locally? → A: Azurite Blob Storage (port 10000) using the same `azure-storage-blob` SDK code path. Container `bronze`, same path structure. Switched via `STORAGE_EMULATOR=true`.
- Q: How should Service Bus be emulated locally? → A: Azure Service Bus emulator running as a Docker container (same emulator used by processing-func). Same CloudEvents envelope, same queue name (`raw-energy-events`). Connection string uses `UseDevelopmentEmulator=true`. Azurite Queue is NOT used for Service Bus emulation — it is only used for Azure Storage Queues (AzureWebJobsStorage). The `STORAGE_EMULATOR` flag controls ADLS/Table storage emulation via Azurite, not Service Bus.
- Q: Storage path — does it include category? → A: Path follows the constitution's partition convention: `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}.csv`. Category is encoded in the filename (`{site_id}_{category}_data.csv`) and in the metadata record, not as a separate path segment.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Bulk Historical CSV Download and Storage (Priority: P1)

As a data engineer, I want to download all historical CSV telemetry files for 4 configured PVDAQ sites from the OEDI Data Lake and store them in ADLS Gen2, so that the raw data is available in a durable, queryable storage layer for downstream processing.

**Why this priority**: Without fetching and storing the raw data, no downstream processing can occur. This is the foundational capability.

**Independent Test**: Can be fully tested by triggering the function and verifying that CSV files for each configured site are downloaded from S3 and written to the correct ADLS path.

**Acceptance Scenarios**:

1. **Given** the system is configured with 4 site IDs (9068, 9069, 2107, 7333), **When** the ingestion function is triggered, **Then** the system discovers all CSV files in each site's `data/` folder on the OEDI S3 bucket.
2. **Given** a site has multiple CSV categories (ac_power, environment, irradiance, tracker, etc.), **When** the system processes that site, **Then** all category CSV files are downloaded and stored in ADLS.
3. **Given** a CSV file is very large (hundreds of megabytes), **When** the system downloads it, **Then** the download uses streaming/chunked reads so that memory usage remains bounded.
4. **Given** a CSV file is downloaded successfully, **When** the worker stores it, **Then** the file is written to the bronze container at the path `source=pvdaq/dataset={site_id}_{category}/ingestion_date=YYYY-MM-DD/{site_id}_{category}_v{version}.csv` alongside a `metadata.json` (ADLS Gen2 in prod, Azurite Blob locally). Each ingestion creates a new version; existing files are never overwritten.
5. **Given** the S3 bucket is temporarily unavailable or returns a server error, **When** the system encounters the error, **Then** it retries with exponential backoff before failing that file.

---

### User Story 2 - Dataset Metadata Registration (Priority: P1)

As a data engineer, I want metadata registered for each ingested dataset so that downstream systems can discover what data is available without scanning storage.

**Why this priority**: Metadata registration is essential for the dataset event and for downstream discovery. Without it, consumers cannot know what was ingested.

**Independent Test**: Can be tested by ingesting a file and verifying the metadata store contains the expected entry with all required fields.

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored in ADLS, **When** metadata is registered, **Then** the `PvdaqFileTracking` table entry includes: site_id, category, source_url, storage_path, file_size, ingestion_time, file_hash, and version. Additionally, a `metadata.json` file is written alongside the CSV containing: dataset_id, source, version, ingestion_time, row_count, checksum, and status ("raw").
2. **Given** a dataset is re-ingested (S3 `LastModified` changed), **When** metadata is registered, **Then** a NEW version is created — a new tracking entity with incremented version and a new `metadata.json`. The previous CSV and its `metadata.json` remain untouched (append-only).

---

### User Story 3 - Dataset Event Emission (Priority: P2)

As a data engineer, I want a dataset-level CloudEvent emitted for each successfully stored file so that downstream processing layers are notified of new data availability.

**Why this priority**: The dataset event is the bridge to the next processing layer. Without it, downstream consumers must poll storage.

**Independent Test**: Can be tested by ingesting a file and verifying a `solar.pvdaq.dataset.available` CloudEvent is emitted to the configured topic.

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored and metadata registered, **When** the worker completes, **Then** a CloudEvent of type `solar.pvdaq.dataset.available` is emitted containing site_id, category, file_format, storage_path, and ingestion_id.
2. **Given** the message bus is temporarily unavailable, **When** event emission fails, **Then** the system retries before marking the dataset as failed.
3. **Given** running locally with the Service Bus emulator, **When** the event is emitted, **Then** it is sent to the `raw-energy-events` queue on the Service Bus emulator using the same CloudEvents envelope.

---

### User Story 4 - Incremental File Detection (Priority: P2)

As a data engineer, I want the system to detect new or updated CSV files on subsequent runs so that only new data is processed, avoiding redundant downloads of previously ingested files.

**Why this priority**: Efficiency optimization — essential for ongoing operation after the initial bulk load.

**Independent Test**: Can be tested by running ingestion, adding a new CSV file to the mock S3 listing, re-running, and verifying only the new file is downloaded.

**Acceptance Scenarios**:

1. **Given** the system has previously ingested all CSV files for a site, **When** a new incremental CSV file appears in the site's data folder, **Then** the system downloads and processes only the new file.
2. **Given** no new files have appeared since the last run, **When** the timer triggers, **Then** no CSV downloads occur and the run completes quickly.
3. **Given** an existing file has a different S3 `LastModified` timestamp than the `LastModified` stored in the file tracking entity (i.e., the file changed on S3 since it was last queued), **When** the dispatcher detects it, **Then** the file is re-queued for download and re-stored in ADLS.

---

### User Story 5 - Observability and Metrics (Priority: P3)

As an operations engineer, I want structured logs and summary metrics emitted after each ingestion run so that I can monitor dataset volume, error rates, and processing health.

**Why this priority**: Operational visibility is important but the system can function without it initially.

**Independent Test**: Can be tested by triggering ingestion and verifying that structured log entries include correlation ID, dataset counts, and duration.

**Acceptance Scenarios**:

1. **Given** an ingestion run completes, **When** metrics are emitted, **Then** the summary includes: datasets_discovered, datasets_downloaded, datasets_stored, datasets_emitted, datasets_failed, and duration.
2. **Given** a site fetch fails with an error, **When** the error is logged, **Then** the log entry includes the site ID, error type, and correlation ID.

---

### Edge Cases

- What happens when a site's `data/` folder on S3 is empty or does not exist? The system logs a warning and continues to the next site.
- What happens when a CSV file contains zero data rows (only headers)? The file is still stored in ADLS and metadata registered — the downstream processing layer decides how to handle empty datasets.
- What happens when the system is interrupted mid-file (e.g., function timeout)? The file tracking store shows status "processing"; the next run re-queues the file for re-download.
- What happens when ADLS write fails? The dataset is marked as failed in the tracking store and dead-lettered with reason "storage_write_failed".
- What happens when a CSV file is corrupt or truncated? The file is stored as-is (raw ingestion does not validate content); metadata includes file_hash for downstream integrity checks.
- What happens when a CSV file exceeds the function's memory limit? Streaming/chunked download and upload ensures only a bounded buffer is in memory at any time.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support a fixed, configured list of 4 PVDAQ site IDs: 9068, 9069, 2107, 7333.
- **FR-002**: System MUST discover all CSV files in each site's OEDI S3 data folder by listing the contents of `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`, paginating through all S3 listing pages using `NextContinuationToken` until `IsTruncated` is false.
- **FR-003**: System MUST download each CSV file using streaming/chunked reads to keep memory usage bounded regardless of file size.
- **FR-004**: System MUST store each downloaded CSV file in the bronze container (`bronze`) at the deterministic path `source=pvdaq/dataset={site_id}_{category}/ingestion_date={YYYY-MM-DD}/{site_id}_{category}_v{version}.csv`. `ingestion_date` is the UTC calendar date at the moment of ingestion (not the S3 LastModified date). `version` is an incrementing integer starting at 1. Each ingestion of the same logical dataset creates a new version file; existing files are never overwritten or deleted (append-only). In production the container is an ADLS Gen2 filesystem named `bronze`; in local development (`STORAGE_EMULATOR=true`) the target is Azurite Blob Storage container `bronze` (port 10000), configured via `ADLS_CONTAINER_NAME=bronze`.
- **FR-005**: System MUST register metadata for each ingested dataset by extending the `PvdaqFileTracking` table entity with: source_url, storage_path, file_size, ingestion_time, file_hash (SHA-256), ingestion_id, and version (integer).
- **FR-006**: System MUST emit a dataset-level CloudEvent of type `solar.pvdaq.dataset.available` indicating that a new dataset is available. The event data block includes: site_id, category, file_format, storage_path, ingestion_id, source_url, file_size, and file_hash (per `contracts/dataset-event.json`). The event MUST be sent to the Service Bus queue named `SERVICE_BUS_QUEUE_NAME` (`raw-energy-events` by default). In production, the Service Bus namespace is identified by `ServiceBusConnection__fullyQualifiedNamespace`; in local development, the same queue is hosted on the Azure Service Bus emulator (Docker). Azure Service Bus Basic tier is used — topics and subscriptions are not available; all communication uses queues.
- **FR-007**: System MUST dead-letter dataset-level failures (download failed, file corrupt, storage write failed) to a dead-letter queue with the file reference, failure reason, and correlation ID.
- **FR-008**: System MUST check each file against a file tracking store before processing and skip files that have already been successfully ingested. The idempotency key is composed of `site_id + category + file_name`.
- **FR-009**: System MUST retry failed HTTP requests to S3 (5xx errors and timeouts) with exponential backoff, up to a configurable maximum number of retries (default: 3).
- **FR-010**: System MUST continue processing remaining sites if one site's data fetch fails, logging the error with the site ID and correlation ID.
- **FR-011**: System MUST emit structured summary metrics after each invocation: datasets_discovered, datasets_downloaded, datasets_stored, datasets_emitted, datasets_failed, and processing duration.
- **FR-012**: System MUST support incremental file detection — on subsequent runs, only CSV files not previously processed (or whose current S3 `LastModified` timestamp differs from the `LastModified` stored in the file tracking entity at queue time) should be downloaded.
- **FR-013**: System MUST be triggerable on a configurable timer schedule via a dispatcher function that lists unprocessed files and enqueues one work item per file.
- **FR-014**: System MUST handle CSV files with varying naming conventions across different measurement categories without requiring per-category configuration.
- **FR-015**: System MUST use a fan-out pattern: a timer-triggered dispatcher function discovers CSV files and enqueues work items; a queue-triggered worker function downloads, stores, registers metadata, and emits a dataset event per file. This ensures each file is processed within function timeout limits regardless of file size.
- **FR-016**: System MUST write a `metadata.json` file alongside each stored CSV at `source=pvdaq/dataset={dataset}/ingestion_date={date}/metadata.json`. The file MUST conform to `contracts/metadata-file.json` and be structured in 7 blocks: `dataset` (identity — dataset_id, version, schema_version, tags), `source` (origin — endpoint, provider, region), `ingestion` (HOW — ingestion_id, batch_id, pipeline, trigger_type, retry_count, checksum, latency, status), `event_time` (WHAT time — null range fields at bronze, `expected_frequency_seconds=300` for PVDAQ), `data_profile` (signals — only `row_count` populated at bronze via newline count; all other fields null), `quality_hint` (all null at bronze), `lineage` (version ancestry). Fields requiring CSV column parsing MUST be null at bronze and populated by the silver layer.
- **FR-017**: System MUST implement versioning: each ingestion of the same logical dataset (`site_id + category`) increments the version counter. Version 1 is the first ingestion. The version is determined by querying `PvdaqFileTracking` for the current maximum version of the dataset before writing. Previous versions MUST NOT be overwritten or deleted.
- **FR-018**: Storage MUST be append-only. The system MUST NOT update, overwrite, or delete previously stored CSV files or their `metadata.json`. Re-ingestion of a changed file creates a new version rather than replacing the existing file.

### Key Entities

- **Site**: A PVDAQ solar installation identified by a numeric site ID (e.g., 9068). Has a name, location metadata, and one or more measurement categories.
- **CSV File**: A comma-separated data file in the OEDI S3 bucket containing timestamped telemetry rows for one site and one measurement category. Named `{site_id}_{category}_data.csv` or `{site_id}_{category}_data_{start}_{end}.csv`.
- **Dataset**: The primary entity of this feature. Represents a single ingested file.
  - site_id
  - category
  - source_url (S3 origin)
  - storage_path (ADLS destination)
  - file_size
  - file_hash (SHA-256)
  - ingestion_id (GUID)
  - ingestion_time
- **Dataset Event**: A CloudEvent of type `solar.pvdaq.dataset.available` emitted per successfully stored dataset. Contains site_id, category, file_format, storage_path, and ingestion_id.
- **File Work Item**: A message on a queue representing a single CSV file to be processed. Contains the site ID, file key/path, and correlation ID. Produced by the dispatcher, consumed by the worker.

## Storage Layout

Container name: **`bronze`** (both production ADLS Gen2 and local Azurite). Configured via `ADLS_CONTAINER_NAME=bronze`. The container IS the bronze layer — no additional `raw/` prefix.

**Naming conventions**:

- `source`: lowercase (e.g. `pvdaq`)
- `dataset`: `{site_id}_{category}` in snake_case (e.g. `9068_ac_power`)
- `ingestion_date`: UTC calendar date of ingestion in `YYYY-MM-DD` format
- file: `{dataset}_v{version}.csv` where version is an incrementing integer (e.g. `9068_ac_power_v1.csv`)

```text
bronze container root/
  source=pvdaq/
    dataset=9068_ac_power/
      ingestion_date=2024-01-15/
        9068_ac_power_v1.csv          ← first ingestion
        metadata.json                  ← written alongside CSV
      ingestion_date=2024-02-03/
        9068_ac_power_v2.csv          ← re-ingestion (S3 file changed)
        metadata.json
    dataset=9068_environment/
      ingestion_date=2024-01-15/
        9068_environment_v1.csv
        metadata.json
    dataset=9069_ac_power/
      ingestion_date=2024-01-15/
        9069_ac_power_v1.csv
        metadata.json
    ...
```

**metadata.json** (written alongside each CSV, full schema in `contracts/metadata-file.json`):

Structured in 7 blocks. Bronze populates fields it can compute without opening the CSV; the silver layer fills in the rest.

```json
{
  "dataset":    { "dataset_id": "9068_ac_power", "dataset_type": "time_series", "version": 1, "schema_version": "unknown", "tags": ["pvdaq","solar"] },
  "source":     { "source": "pvdaq", "source_type": "s3_public", "endpoint": "pvdaq/2023-solar-data-prize/9068_OEDI/data/", "provider": "NREL", "region": "us-east-1" },
  "ingestion":  { "ingestion_time": "2024-01-15T08:32:00Z", "ingestion_id": "uuid...", "batch_id": "correlation-id...", "pipeline": "energy-ingestion-boundary-v1", "trigger_type": "scheduled", "retry_count": 0, "source_file_name": "9068_ac_power_data.csv", "file_size_bytes": 65000000, "checksum": "a3f1...64hex", "ingestion_latency_seconds": 47.3, "status": "success" },
  "event_time": { "event_time_start": null, "event_time_end": null, "expected_frequency_seconds": 300, "expected_records": null },
  "data_profile": { "row_count": 105121, "null_percentage": null, "duplicate_rows": null, "min_timestamp": null, "max_timestamp": null, "schema_detected": null, "corrupted_rows": null },
  "quality_hint": { "basic_quality_score": null, "schema_valid": null, "time_continuity_suspected_gap": null, "notes": [] },
  "lineage":    { "parent_dataset_version": null, "rerun_of": null, "related_incident_id": null }
}
```

Fields null at bronze (`event_time_start/end`, `data_profile.*` except `row_count`, all `quality_hint.*` except `notes`) require CSV column parsing — intentionally deferred to the silver layer. `row_count` is the newline character count during streaming (no CSV interpretation). `expected_frequency_seconds` is the PVDAQ domain constant (300 s = 5-minute intervals).

## Local vs Production Behaviour

| Concern        | Local                                                         | Production                                                                 |
| -------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Bronze storage | Azurite Blob port 10000 (`bronze`)                            | ADLS Gen2 bronze container                                                 |
| Event emission | Service Bus emulator (Docker) — queue `raw-energy-events`     | Service Bus queue `raw-energy-events` (`jorgevr.servicebus.windows.net`)   |
| Work queue     | Service Bus emulator (Docker) — queue `pvdaq-historical-work` | Service Bus queue `pvdaq-historical-work`                                  |
| Dead letter    | Service Bus emulator (Docker) — queue `pvdaq-dead-letter`     | Service Bus queue `pvdaq-dead-letter`                                      |
| Table storage  | Azurite Table port 10002                                      | Azure Table Storage                                                        |
| Auth           | Connection string (devstoreaccount1) / emulator connection    | `ServiceBusConnection__fullyQualifiedNamespace` + `DefaultAzureCredential` |

> **Note**: Azure Service Bus Basic tier is in use. Topics and subscriptions are not available. All inter-service communication uses queues.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All historical CSV files for the 4 configured sites are successfully downloaded and stored in ADLS across dispatcher + worker invocations (allowing for retries on transient errors).
- **SC-002**: 100% of discovered datasets are successfully stored in ADLS Gen2 raw container at the correct deterministic path.
- **SC-003**: Each dataset ingestion produces a `solar.pvdaq.dataset.available` CloudEvent emitted to the configured message bus topic.
- **SC-004**: Zero duplicate dataset events are emitted across multiple runs — the file tracking store prevents re-processing of already-ingested files.
- **SC-005**: A single site's failure does not prevent processing of the remaining 3 sites — partial success is acceptable and logged.
- **SC-006**: Memory usage remains bounded during download and upload of large CSV files (hundreds of megabytes) due to streaming reads/writes.
- **SC-007**: Each ingestion run produces a structured summary log entry with all FR-011 metric fields.
- **SC-008**: Subsequent runs after initial bulk load complete significantly faster by skipping previously processed files.

## Assumptions

- The OEDI S3 bucket (`oedi-data-lake`) remains publicly accessible over HTTPS without authentication.
- CSV file encoding is UTF-8.
- The S3 bucket supports XML-based listing responses for enumerating objects within a prefix, including paginated responses via `NextContinuationToken`.
- The 4 target sites are stable and their data folders follow the convention `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`.
- Different measurement categories (ac_power, environment, irradiance, etc.) have varying column sets, but this feature does not parse or validate CSV content — it stores files as-is.
- The timer schedule is configurable via environment variables and defaults to `0 0 */6 * * *` (every 6 hours).
- Worker function concurrency is controlled exclusively by `maxConcurrentCalls` in `host.json` (Azure Functions host-level setting); no application-level semaphore is used. Per-file idempotency prevents duplicate work if concurrent workers race on the same file.
- The `tenant_id` field in emitted CloudEvents defaults to `"default"` (single-tenant deployment for PVDAQ).
- Dead-letter queue and message bus topic are pre-provisioned infrastructure — the system does not create them.
- ADLS Gen2 storage account with hierarchical namespace enabled is pre-provisioned.
- This feature is implemented as a new, separate function within the same function app as feature 001. It reuses shared modules (Service Bus emitter, config patterns, observability) but has its own trigger, entry point, and OEDI client tailored to the 2023-solar-data-prize path structure.
- **Row-level processing is out of scope**: CSV parsing, row validation, per-row CloudEvent emission, schema normalization, per-row dead-lettering, and per-row idempotency are responsibilities of a downstream processing layer (feature 003+).
- **Feature 001 migration complete**: Feature 001 (daily PVDAQ polling) has migrated to the same dataset-level pattern as this feature (spec updated 2026-03-18). Both features share the same bronze storage path convention and `solar.pvdaq.dataset.available` event type.

## Target Sites

| Site Name | Site ID | Metadata URL |
| --- | --- | --- |
| SR_CO | 9068 | <https://openei.org/wiki/PVDAQ/Sites/SR_CO> |
| Simon_Solar_Farm | 9069 | <https://openei.org/wiki/PVDAQ/Sites/Simon_Solar_Farm> |
| Farm_Solar_Array | 2107 | <https://openei.org/wiki/PVDAQ/Sites/Farm_Solar_Array> |
| Shine_On_Solar_Facility | 7333 | <https://openei.org/wiki/PVDAQ/Sites/Shine_On_Solar_Facility> |

## Data Source

- **Bucket**: `https://oedi-data-lake.s3.amazonaws.com`
- **Path pattern**: `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`
- **File naming**: `{site_id}_{category}_data.csv` (historical), `{site_id}_{category}_data_{start}_{end}.csv` (incremental)
- **Known categories**: ac_power, acvolt_curr, dc_combiner, environment, irradiance, meter, meter_pf, tracker, electrical, meter_15m
- **Access**: Public HTTPS, no authentication required
- **File sizes**: Range from ~7 MB to ~870 MB per file
- **Measurement resolution**: 5-minute intervals

## What Moved Downstream

The following responsibilities previously in this spec are now in the processing layer (feature 003+):

| Responsibility | Old FR | New Location |
| -------------- | ------ | ------------ |
| CSV row parsing & normalization | FR-004 (old) | Processing layer |
| Row-level schema validation | FR-005 (old) | Processing layer |
| Per-row CloudEvent emission | FR-006 (old) | Processing layer |
| Per-row dead-lettering | FR-007 (old) | Processing layer |
| Per-row idempotency | FR-008 (old) | Processing layer |

**Event granularity change**: From millions of per-row events to tens of per-dataset events, dramatically improving system stability and cost efficiency.
