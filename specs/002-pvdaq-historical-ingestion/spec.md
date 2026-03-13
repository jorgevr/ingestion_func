# Feature Specification: PVDAQ Historical Dataset Ingestion

**Feature Branch**: `002-pvdaq-historical-ingestion`
**Created**: 2026-03-02
**Updated**: 2026-03-09
**Status**: Draft
**Input**: User description: "Discover, download, and store historical photovoltaic CSV files from the OEDI Data Lake for 4 specific PVDAQ sites into ADLS Gen2, registering metadata and emitting a dataset-level CloudEvent per file."

## Clarifications

### Session 2026-03-02

- Q: What is the relationship between this feature (002) and the existing feature 001 pipeline? → A: New separate function in the same function app — reuses shared modules (emitter, config patterns), independent trigger.
- Q: How should the system handle Azure Function timeout limits given CSV files up to 870 MB? → A: Fan-out pattern — timer-triggered function lists files and dispatches one message per file to a queue; a queue-triggered function processes each file independently.

### Session 2026-03-09 — Architecture Revision

- Q: What is the event granularity? → A: **One CloudEvent per dataset (file)**, not per row. Row-level parsing, validation, schema normalization, and per-row emission move to a downstream processing layer (feature 003+). This feature is now **dataset ingestion only**: discover → download → store → register → emit dataset event.
- Q: How should idempotency work? → A: Per-file idempotency keyed on `site_id + category + file_name`. Much simpler and cheaper than per-row tracking.
- Q: Where are files stored? → A: Azure Data Lake Storage Gen2 raw container using a deterministic path: `/raw/pvdaq/site_id={site_id}/category={category}/{file_name}.csv`.
- Q: Where is dataset metadata stored? → A: Extend the existing `PvdaqFileTracking` Azure Table with new columns (storage_path, file_hash, ingestion_id) rather than creating a separate store. Keeps file tracking and metadata in one table.
- Q: Does feature 001 continue with per-row processing? → A: Feature 001 will also migrate to dataset-level ingestion in a future phase. For now, feature 001 continues as-is; this spec (002) establishes the dataset-level pattern that 001 will adopt later.
- Q: What hash algorithm for file_hash? → A: SHA-256 — consistent with the existing `IdempotencyStore._row_key()` which already uses SHA-256, and provides strong integrity guarantees for large files.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Bulk Historical CSV Download and Storage (Priority: P1)

As a data engineer, I want to download all historical CSV telemetry files for 4 configured PVDAQ sites from the OEDI Data Lake and store them in ADLS Gen2, so that the raw data is available in a durable, queryable storage layer for downstream processing.

**Why this priority**: Without fetching and storing the raw data, no downstream processing can occur. This is the foundational capability.

**Independent Test**: Can be fully tested by triggering the function and verifying that CSV files for each configured site are downloaded from S3 and written to the correct ADLS path.

**Acceptance Scenarios**:

1. **Given** the system is configured with 4 site IDs (9068, 9069, 2107, 7333), **When** the ingestion function is triggered, **Then** the system discovers all CSV files in each site's `data/` folder on the OEDI S3 bucket.
2. **Given** a site has multiple CSV categories (ac_power, environment, irradiance, tracker, etc.), **When** the system processes that site, **Then** all category CSV files are downloaded and stored in ADLS.
3. **Given** a CSV file is very large (hundreds of megabytes), **When** the system downloads it, **Then** the download uses streaming/chunked reads so that memory usage remains bounded.
4. **Given** a CSV file is downloaded successfully, **When** the worker stores it, **Then** the file is written to ADLS at the path `/raw/pvdaq/site_id={site_id}/category={category}/{file_name}.csv`.
5. **Given** the S3 bucket is temporarily unavailable or returns a server error, **When** the system encounters the error, **Then** it retries with exponential backoff before failing that file.

---

### User Story 2 - Dataset Metadata Registration (Priority: P1)

As a data engineer, I want metadata registered for each ingested dataset so that downstream systems can discover what data is available without scanning storage.

**Why this priority**: Metadata registration is essential for the dataset event and for downstream discovery. Without it, consumers cannot know what was ingested.

**Independent Test**: Can be tested by ingesting a file and verifying the metadata store contains the expected entry with all required fields.

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored in ADLS, **When** metadata is registered, **Then** the metadata entry includes: site_id, category, source_url, storage_path, file_size, ingestion_time, and file_hash.
2. **Given** a dataset is re-ingested (same file, updated content), **When** metadata is registered, **Then** the existing entry is updated with the new ingestion_time and file_hash.

---

### User Story 3 - Dataset Event Emission (Priority: P2)

As a data engineer, I want a dataset-level CloudEvent emitted for each successfully stored file so that downstream processing layers are notified of new data availability.

**Why this priority**: The dataset event is the bridge to the next processing layer. Without it, downstream consumers must poll storage.

**Independent Test**: Can be tested by ingesting a file and verifying a `solar.pvdaq.dataset.available` CloudEvent is emitted to the configured topic.

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored in ADLS and metadata registered, **When** the worker completes, **Then** a CloudEvent of type `solar.pvdaq.dataset.available` is emitted containing site_id, category, file_format, storage_path, and ingestion_id.
2. **Given** the message bus is temporarily unavailable, **When** event emission fails, **Then** the system retries before marking the dataset as failed.

---

### User Story 4 - Incremental File Detection (Priority: P2)

As a data engineer, I want the system to detect new or updated CSV files on subsequent runs so that only new data is processed, avoiding redundant downloads of previously ingested files.

**Why this priority**: Efficiency optimization — essential for ongoing operation after the initial bulk load.

**Independent Test**: Can be tested by running ingestion, adding a new CSV file to the mock S3 listing, re-running, and verifying only the new file is downloaded.

**Acceptance Scenarios**:

1. **Given** the system has previously ingested all CSV files for a site, **When** a new incremental CSV file appears in the site's data folder, **Then** the system downloads and processes only the new file.
2. **Given** no new files have appeared since the last run, **When** the timer triggers, **Then** no CSV downloads occur and the run completes quickly.
3. **Given** an existing file has changed (different size or last-modified), **When** the dispatcher detects it, **Then** the file is re-queued for download and re-stored in ADLS.

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
- **FR-002**: System MUST discover all CSV files in each site's OEDI S3 data folder by listing the contents of `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`.
- **FR-003**: System MUST download each CSV file using streaming/chunked reads to keep memory usage bounded regardless of file size.
- **FR-004**: System MUST store each downloaded CSV file in Azure Data Lake Storage Gen2 raw container using a deterministic path pattern: `/raw/pvdaq/site_id={site_id}/category={category}/{file_name}.csv`.
- **FR-005**: System MUST register metadata for each ingested dataset by extending the `PvdaqFileTracking` table entity with: source_url, storage_path, file_size, ingestion_time, file_hash (SHA-256), and ingestion_id.
- **FR-006**: System MUST emit a dataset-level CloudEvent of type `solar.pvdaq.dataset.available` indicating that a new dataset is available. The event data block includes: site_id, category, file_format, storage_path, ingestion_id, source_url, file_size, and file_hash (per `contracts/dataset-event.json`).
- **FR-007**: System MUST dead-letter dataset-level failures (download failed, file corrupt, storage write failed) to a dead-letter queue with the file reference, failure reason, and correlation ID.
- **FR-008**: System MUST check each file against a file tracking store before processing and skip files that have already been successfully ingested. The idempotency key is composed of `site_id + category + file_name`.
- **FR-009**: System MUST retry failed HTTP requests to S3 (5xx errors and timeouts) with exponential backoff, up to a configurable maximum number of retries.
- **FR-010**: System MUST continue processing remaining sites if one site's data fetch fails, logging the error with the site ID and correlation ID.
- **FR-011**: System MUST emit structured summary metrics after each invocation: datasets_discovered, datasets_downloaded, datasets_stored, datasets_emitted, datasets_failed, and processing duration.
- **FR-012**: System MUST support incremental file detection — on subsequent runs, only CSV files not previously processed (or changed) should be downloaded.
- **FR-013**: System MUST be triggerable on a configurable timer schedule via a dispatcher function that lists unprocessed files and enqueues one work item per file.
- **FR-014**: System MUST handle CSV files with varying naming conventions across different measurement categories without requiring per-category configuration.
- **FR-015**: System MUST use a fan-out pattern: a timer-triggered dispatcher function discovers CSV files and enqueues work items; a queue-triggered worker function downloads, stores, registers metadata, and emits a dataset event per file. This ensures each file is processed within function timeout limits regardless of file size.

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

ADLS Gen2 raw container — medallion architecture raw layer:

```text
adls://{storage-account}/raw/pvdaq/
  site_id=9068/
    category=ac_power/
      9068_ac_power_data.csv
    category=irradiance/
      9068_irradiance_data.csv
    category=environment/
      9068_environment_data.csv
  site_id=9069/
    category=ac_power/
      9069_ac_power_data.csv
    ...
```

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
- The S3 bucket supports XML-based listing responses for enumerating objects within a prefix.
- The 4 target sites are stable and their data folders follow the convention `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`.
- Different measurement categories (ac_power, environment, irradiance, etc.) have varying column sets, but this feature does not parse or validate CSV content — it stores files as-is.
- The timer schedule is configurable via environment variables and defaults to `0 0 */6 * * *` (every 6 hours).
- The `tenant_id` field in emitted CloudEvents defaults to `"default"` (single-tenant deployment for PVDAQ).
- Dead-letter queue and message bus topic are pre-provisioned infrastructure — the system does not create them.
- ADLS Gen2 storage account with hierarchical namespace enabled is pre-provisioned.
- This feature is implemented as a new, separate function within the same function app as feature 001. It reuses shared modules (Service Bus emitter, config patterns, observability) but has its own trigger, entry point, and OEDI client tailored to the 2023-solar-data-prize path structure.
- **Row-level processing is out of scope**: CSV parsing, row validation, per-row CloudEvent emission, schema normalization, per-row dead-lettering, and per-row idempotency are responsibilities of a downstream processing layer (feature 003+).
- **Feature 001 future migration**: Feature 001 (daily PVDAQ polling) continues with per-row processing for now but will migrate to the dataset-level pattern established by this feature in a future phase.

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
