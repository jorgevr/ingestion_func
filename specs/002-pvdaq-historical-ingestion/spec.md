# Feature Specification: PVDAQ Historical Data Ingestion

**Feature Branch**: `002-pvdaq-historical-ingestion`
**Created**: 2026-03-02
**Status**: Draft
**Input**: User description: "Download historical photovoltaic telemetry CSV files from the OEDI Data Lake for 4 specific PVDAQ sites and emit each valid record as a CloudEvents envelope to a message bus."

## Clarifications

### Session 2026-03-02

- Q: How should the idempotency key be composed given that multiple CSV categories per site may share the same timestamp? → A: Key = `site_id + category + timestamp` — each category's row is a separate record.
- Q: What is the relationship between this feature (002) and the existing feature 001 pipeline? → A: New separate function in the same function app — reuses shared modules (emitter, idempotency store, config patterns), independent trigger.
- Q: How should the system handle Azure Function timeout limits given CSV files up to 870 MB? → A: Fan-out pattern — timer-triggered function lists files and dispatches one message per file to a queue; a queue-triggered function processes each file independently.
- Q: What is the CloudEvent emission granularity given potentially millions of rows per file? → A: One CloudEvent per CSV row — fine-grained, matches existing feature 001 pattern. Service Bus batched sends reduce round-trips.
- Q: How should the system identify the timestamp column across different CSV categories? → A: Auto-detect from a priority list of known timestamp column names (e.g., `measured_on`, `timestamp`, `Date-Time`). Use the first match found in the CSV headers.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Bulk Historical CSV Download (Priority: P1)

As a data engineer, I want to download all historical CSV telemetry files for 4 configured PVDAQ sites from the OEDI Data Lake so that the data is available for downstream analytics and processing.

**Why this priority**: Without fetching the raw data, no downstream processing, validation, or emission can occur. This is the foundational capability.

**Independent Test**: Can be fully tested by triggering the function and verifying that CSV files for each configured site are fetched and parsed into records. Delivers raw telemetry data from the public S3 bucket.

**Acceptance Scenarios**:

1. **Given** the system is configured with 4 site IDs (9068, 9069, 2107, 7333), **When** the ingestion function is triggered, **Then** the system discovers all CSV files in each site's `data/` folder on the OEDI S3 bucket.
2. **Given** a site has multiple CSV categories (ac_power, environment, irradiance, tracker, etc.), **When** the system processes that site, **Then** all category CSV files are downloaded and parsed.
3. **Given** a CSV file is very large (hundreds of megabytes), **When** the system downloads it, **Then** the download uses streaming/chunked reads so that memory usage remains bounded.
4. **Given** the S3 bucket is temporarily unavailable or returns a server error, **When** the system encounters the error, **Then** it retries with exponential backoff before failing that file.

---

### User Story 2 - Record Validation and Emission (Priority: P2)

As a data engineer, I want each CSV row to be validated against a schema and emitted as a CloudEvents message to a message bus, so that downstream consumers receive only well-formed telemetry records.

**Why this priority**: Validation and emission are the core value delivery — transforming raw CSV rows into structured, reliable event streams.

**Independent Test**: Can be tested by providing sample CSV data (valid and invalid rows) and verifying that valid records produce CloudEvents messages on the topic and invalid records are routed to a dead-letter queue.

**Acceptance Scenarios**:

1. **Given** a CSV row contains all required fields (site ID, timestamp, at least one measurement), **When** the row is processed, **Then** the system emits a CloudEvents envelope containing the normalized record to the configured message bus topic.
2. **Given** a CSV row is missing a required field (e.g., no timestamp), **When** the row is processed, **Then** the system sends the invalid record to the dead-letter queue with a reason.
3. **Given** the message bus is temporarily unavailable, **When** emission fails, **Then** the system retries before logging the failure and continuing with remaining records.

---

### User Story 3 - Idempotent Processing (Priority: P2)

As a data engineer, I want the system to track which records have already been emitted so that re-running the ingestion does not produce duplicate messages.

**Why this priority**: Historical data is static — re-runs must not flood downstream consumers with duplicates. Equal priority to emission since both are needed for production reliability.

**Independent Test**: Can be tested by running ingestion twice for the same site and verifying that the second run emits zero records (all marked as duplicates).

**Acceptance Scenarios**:

1. **Given** a record has never been processed before, **When** the system processes it, **Then** it is marked as "pending" in the idempotency store, emitted, then marked as "completed."
2. **Given** a record was previously emitted (status = completed), **When** the system encounters it again on a re-run, **Then** it is skipped without emission.
3. **Given** a record was reserved but never completed (status = pending from a previous crash), **When** the system encounters it again, **Then** it re-emits the record and marks it as completed.

---

### User Story 4 - Incremental File Detection (Priority: P3)

As a data engineer, I want the system to detect new or updated CSV files on subsequent runs so that only new data is processed, avoiding redundant downloads of previously ingested files.

**Why this priority**: Efficiency optimization — not required for initial bulk load but essential for ongoing operation.

**Independent Test**: Can be tested by running ingestion, adding a new CSV file to the mock S3 listing, re-running, and verifying only the new file is downloaded.

**Acceptance Scenarios**:

1. **Given** the system has previously ingested all CSV files for a site, **When** a new incremental CSV file appears in the site's data folder, **Then** the system downloads and processes only the new file.
2. **Given** no new files have appeared since the last run, **When** the timer triggers, **Then** no CSV downloads occur and the run completes quickly.

---

### User Story 5 - Observability and Metrics (Priority: P3)

As an operations engineer, I want structured logs and summary metrics emitted after each ingestion run so that I can monitor data volume, error rates, and processing health.

**Why this priority**: Operational visibility is important but the system can function without it initially.

**Independent Test**: Can be tested by triggering ingestion and verifying that structured log entries include correlation ID, record counts (retrieved, valid, invalid, emitted, duplicates), and duration.

**Acceptance Scenarios**:

1. **Given** an ingestion run completes, **When** metrics are emitted, **Then** the summary includes: source, total records retrieved, valid count, invalid count, emitted count, duplicate count, and duration.
2. **Given** a site fetch fails with an error, **When** the error is logged, **Then** the log entry includes the site ID, error type, and correlation ID.

---

### Edge Cases

- What happens when a site's `data/` folder on S3 is empty or does not exist? The system logs a warning and continues to the next site.
- What happens when a CSV file contains zero data rows (only headers)? The system logs the empty file and moves on without error.
- What happens when a CSV column header has an unexpected format or encoding? The system treats unrecognized columns as opaque string fields and includes them in the record.
- What happens when the system is interrupted mid-file (e.g., function timeout)? Records already emitted are tracked by idempotency; un-emitted records from that file will be retried on the next run.
- What happens when two CSV files in the same site contain overlapping timestamps? The idempotency store prevents duplicate emission regardless of source file.
- What happens when a CSV file exceeds the function's memory limit? Streaming/chunked processing ensures only a bounded number of rows are in memory at any time.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST support a fixed, configured list of 4 PVDAQ site IDs: 9068, 9069, 2107, 7333.
- **FR-002**: System MUST discover all CSV files in each site's OEDI S3 data folder by listing the contents of `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`.
- **FR-003**: System MUST download each CSV file using streaming/chunked reads to keep memory usage bounded regardless of file size.
- **FR-004**: System MUST parse each CSV file and normalize records: inject `SiteID` (integer) from the S3 folder path (since the 2023-solar-data-prize CSVs do not contain a `system_id` column), auto-detect the timestamp column from a priority list of known names (e.g., `measured_on`, `timestamp`, `Date-Time`) and map it to `measdatetime` (string), strip sensor ID suffixes from column names (patterns: `_o_\d+` and `_\d+`), and cast numeric values.
- **FR-005**: System MUST validate each record against a defined schema requiring at minimum: `SiteID` (integer) and `measdatetime` (non-empty string).
- **FR-006**: System MUST emit each valid record as a CloudEvents v1.0 envelope to a configured message bus topic, including metadata: source, tenant ID, schema version, mapping version, and correlation ID.
- **FR-007**: System MUST route invalid records to a dead-letter queue with the original record payload and a human-readable reason for rejection.
- **FR-008**: System MUST check each record against an idempotency store before emission and skip records that have already been successfully emitted. The idempotency key is composed of `site_id + category + timestamp`, ensuring records from different measurement categories with the same timestamp are treated as distinct.
- **FR-009**: System MUST retry failed HTTP requests to S3 (5xx errors and timeouts) with exponential backoff, up to a configurable maximum number of retries.
- **FR-010**: System MUST continue processing remaining sites if one site's data fetch fails, logging the error with the site ID and correlation ID.
- **FR-011**: System MUST emit structured summary metrics after each invocation: source, total records retrieved, valid count, invalid count, emitted count, duplicate count, and processing duration.
- **FR-012**: System MUST support incremental file detection — on subsequent runs, only CSV files not previously processed should be downloaded.
- **FR-013**: System MUST be triggerable on a configurable timer schedule via a dispatcher function that lists unprocessed files and enqueues one work item per file.
- **FR-014**: System MUST handle CSV files with varying column sets across different measurement categories (ac_power, environment, irradiance, tracker, etc.) without requiring per-category configuration.
- **FR-015**: System MUST use a fan-out pattern: a timer-triggered dispatcher function discovers CSV files and enqueues work items; a queue-triggered worker function processes one CSV file per invocation. This ensures each file is processed within function timeout limits regardless of file size.

### Key Entities

- **Site**: A PVDAQ solar installation identified by a numeric site ID (e.g., 9068). Has a name, location metadata, and one or more measurement categories.
- **CSV File**: A comma-separated data file in the OEDI S3 bucket containing timestamped telemetry rows for one site and one measurement category. Named `{site_id}_{category}_data.csv` or `{site_id}_{category}_data_{start}_{end}.csv`.
- **Telemetry Record**: A single row from a CSV file, normalized into a dictionary with `SiteID`, `measdatetime`, and zero or more measurement fields.
- **CloudEvents Envelope**: A structured message wrapping a telemetry record with standard metadata (source, type, subject, time, correlation ID, schema version).
- **Idempotency Entry**: A record in the idempotency store keyed by site ID + measurement category + timestamp, tracking whether a telemetry record has been emitted (pending, completed).
- **File Work Item**: A message on a queue representing a single CSV file to be processed. Contains the site ID, file key/path, and correlation ID. Produced by the dispatcher, consumed by the worker.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: All historical CSV files for the 4 configured sites are successfully downloaded and parsed across dispatcher + worker invocations (allowing for retries on transient errors).
- **SC-002**: 100% of valid telemetry records are emitted as CloudEvents messages to the message bus topic.
- **SC-003**: 100% of invalid records are routed to the dead-letter queue with a descriptive rejection reason.
- **SC-004**: Zero duplicate messages are emitted across multiple runs of the same data — the idempotency store prevents re-emission.
- **SC-005**: A single site's failure does not prevent processing of the remaining 3 sites — partial success is acceptable and logged.
- **SC-006**: Memory usage remains bounded during processing of large CSV files (hundreds of megabytes) due to streaming reads.
- **SC-007**: Each ingestion run produces a structured summary log entry with all FR-011 metric fields.
- **SC-008**: Subsequent runs after initial bulk load complete significantly faster by skipping previously processed files.

## Assumptions

- The OEDI S3 bucket (`oedi-data-lake`) remains publicly accessible over HTTPS without authentication.
- CSV file encoding is UTF-8.
- The S3 bucket supports XML-based listing responses for enumerating objects within a prefix.
- The 4 target sites are stable and their data folders follow the convention `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`.
- Different measurement categories (ac_power, environment, irradiance, etc.) share a common structure: a timestamp column and one or more measurement columns, though specific column names vary.
- The timer schedule is configurable via environment variables and defaults to a reasonable interval (e.g., every 15 minutes).
- Dead-letter queue and message bus topic are pre-provisioned infrastructure — the system does not create them.
- This feature is implemented as a new, separate function within the same function app as feature 001. It reuses shared modules (Service Bus emitter, idempotency store, observability, config patterns) but has its own trigger, entry point, and OEDI client tailored to the 2023-solar-data-prize path structure.

## Target Sites

| Site Name               | Site ID | Metadata URL                                               |
| ----------------------- | ------- | ---------------------------------------------------------- |
| SR_CO                   | 9068    | <https://openei.org/wiki/PVDAQ/Sites/SR_CO>                |
| Simon_Solar_Farm        | 9069    | <https://openei.org/wiki/PVDAQ/Sites/Simon_Solar_Farm>     |
| Farm_Solar_Array        | 2107    | <https://openei.org/wiki/PVDAQ/Sites/Farm_Solar_Array>     |
| Shine_On_Solar_Facility | 7333    | <https://openei.org/wiki/PVDAQ/Sites/Shine_On_Solar_Facility> |

## Data Source

- **Bucket**: `https://oedi-data-lake.s3.amazonaws.com`
- **Path pattern**: `pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`
- **File naming**: `{site_id}_{category}_data.csv` (historical), `{site_id}_{category}_data_{start}_{end}.csv` (incremental)
- **Known categories**: ac_power, acvolt_curr, dc_combiner, environment, irradiance, meter, meter_pf, tracker, electrical, meter_15m
- **Access**: Public HTTPS, no authentication required
- **File sizes**: Range from ~7 MB to ~870 MB per file
- **Measurement resolution**: 5-minute intervals
