# Feature Specification: PVDAQ Ingestion

**Feature Branch**: `001-pvdaq-ingestion`
**Created**: 2026-02-19
**Updated**: 2026-03-18
**Status**: Revised — dataset-level ingestion
**Input**: User description: "PVDAQ Ingestion — boundary-only ingestion of NREL PVDAQ photovoltaic telemetry"

## Revision History

| Version | Date | Change |
| ------- | ---------- | ------ |
| 1.0 | 2026-02-19 | Initial — per-row retrieval, validation, and emission |
| 2.0 | 2026-03-18 | Migrated to dataset-level pattern: download → store bronze → emit dataset event. Row-level parsing/validation/emission moved downstream. |

## Clarifications

### Session 2026-02-19

- Q: Envelope structure — custom envelope or CloudEvents-compatible per constitution? → A: CloudEvents-compatible envelope. Core fields: `type`, `source`, `id`, `time`, `datacontenttype`. Extension attributes: `tenant_id`, `source_vendor`, `schema_version`, `mapping_version`, `correlation_id`, `ingestion_timestamp`.
- Q: Expected data volume per invocation and polling model? → A: Phase 1 targets 1–10 sites, up to 1,000 records per site per poll. Sites polled sequentially to respect NREL rate limits.

### Session 2026-03-18 — Architecture Revision

- Q: What is the event granularity? → A: **One CloudEvent per downloaded CSV file (dataset)**, not per row. Row-level parsing, schema validation, normalization, and per-row emission move to a downstream processing layer (feature 003+). This feature is now: poll → download CSV → store raw file to ADLS bronze → register metadata → emit `solar.pvdaq.dataset.available`.
- Q: Where are files stored? → A: ADLS Gen2 raw container (bronze layer) using path: `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}.csv`. Locally, Azurite Blob Storage emulates ADLS Gen2 using the same SDK code path.
- Q: How is local Service Bus emulated? → A: Azurite Queue (port 10001) replaces Service Bus when `STORAGE_EMULATOR=true`. Same event envelope, different transport. Code uses an abstraction that switches based on the environment variable.
- Q: File-level idempotency key? → A: Composite of `site_id + date_window` derived from the poll parameters. If the same site+date combination has already been stored, the file write and event emission are skipped.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Scheduled CSV Download and Bronze Storage (Priority: P1)

The system retrieves photovoltaic telemetry CSV data from the NREL PVDAQ
dataset on a recurring schedule and stores the raw file in the ADLS Gen2
bronze layer (or Azurite Blob locally) without parsing rows.

**Why this priority**: Storing the raw file is the foundational step. Without
it, no downstream processing can occur.

**Independent Test**: Trigger the function with a mock PVDAQ API response.
Verify a CSV file appears at the correct ADLS/Azurite Blob path with intact
content and a matching SHA-256 hash.

**Acceptance Scenarios**:

1. **Given** a configured CRON schedule and site ID list, **When** the timer
   fires, **Then** the function calls the PVDAQ API for each site within the
   configured time window and stores the resulting CSV file at
   `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}.csv`.
2. **Given** the PVDAQ API returns zero records for a site, **When** the
   function processes that site, **Then** no file is written and a structured
   log entry records "0 records retrieved" for that site.
3. **Given** the PVDAQ API is unreachable, **When** the function attempts
   retrieval, **Then** it retries with exponential backoff (max 3 attempts)
   and logs a failure with correlation ID after exhaustion.
4. **Given** a large CSV response, **When** the function writes it to storage,
   **Then** the write uses streaming so memory usage remains bounded.

---

### User Story 2 — Dataset Metadata Registration (Priority: P1)

Metadata for each stored dataset is registered in the file tracking store so
downstream systems can discover available data without scanning storage.

**Why this priority**: Without metadata, consumers cannot know what was
ingested or verify file integrity.

**Independent Test**: Ingest a site's CSV and verify the file tracking table
contains an entry with all required fields (site_id, storage_path, file_size,
file_hash, ingestion_time, ingestion_id).

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored, **When** metadata is registered,
   **Then** the entry includes: site_id, source_url, storage_path, file_size,
   ingestion_time (UTC), file_hash (SHA-256), and ingestion_id (GUID).
2. **Given** a site is polled again with the same date window, **When** an
   existing idempotency record is found, **Then** no write or metadata
   registration occurs and the function returns success.

---

### User Story 3 — Dataset Event Emission (Priority: P1)

After a CSV file is successfully stored and metadata registered, the function
emits a single `solar.pvdaq.dataset.available` CloudEvent so downstream
consumers are notified without polling storage.

**Why this priority**: The dataset event is the signal that triggers all
downstream processing. Emission is inseparable from the storage step.

**Independent Test**: Ingest a file and verify one `solar.pvdaq.dataset.available`
CloudEvent appears on the configured topic/queue containing the correct
site_id, storage_path, file_hash, and ingestion_id.

**Acceptance Scenarios**:

1. **Given** a CSV file has been stored and metadata registered, **When** the
   function emits the event, **Then** a CloudEvents-compatible message is
   published with: `type` = `solar.pvdaq.dataset.available`, `source` =
   `/energy-ingestion-boundary/pvdaq`, `id` = UUID, `time` = UTC ISO-8601,
   `datacontenttype` = `application/json`; extension attributes `tenant_id`,
   `source_vendor` = "PVDAQ", `schema_version`, `correlation_id`,
   `ingestion_timestamp`; `data` containing site_id, storage_path, file_size,
   file_hash, ingestion_id, and source_url.
2. **Given** the message bus publish fails, **When** the function retries,
   **Then** it uses exponential backoff and dead-letters after exhaustion.
3. **Given** `STORAGE_EMULATOR=true`, **When** the event is emitted, **Then**
   it is sent to an Azurite Queue instead of Service Bus, using the same
   CloudEvents envelope.

---

### User Story 4 — File-level Idempotency (Priority: P2)

The function prevents duplicate downloads and emissions when the same site
and date window are encountered more than once (retries, overlapping polls).

**Why this priority**: Required by the constitution but ranked below core
storage/emission because it builds on a working pipeline.

**Independent Test**: Run the function for a site+date combination twice.
Verify the file is written and the event emitted exactly once; the second
run returns success without any write or emission.

**Acceptance Scenarios**:

1. **Given** a site+date window not previously seen, **When** the function
   processes it, **Then** the idempotency record is written and the dataset
   is stored and emitted.
2. **Given** a site+date window whose idempotency key exists, **When** the
   function processes it, **Then** no storage write and no event emission
   occur.

---

### User Story 5 — Structured Observability (Priority: P2)

Every invocation emits structured telemetry so operators can monitor
ingestion health.

**Acceptance Scenarios**:

1. **Given** a successful invocation, **When** processing completes, **Then**
   structured telemetry includes: `source` = "PVDAQ", `sites_processed`,
   `datasets_stored`, `datasets_skipped`, `datasets_failed`, `duration_ms`,
   and `correlation_id`.
2. **Given** an unhandled exception, **When** the function fails, **Then** it
   is logged as structured JSON with `correlation_id`, `function_name`, and
   `vendor`.

---

### Edge Cases

- What happens when **ADLS/Azurite write fails**? The dataset is marked as
  failed; no event is emitted (write-before-emit enforced).
- What happens when **all sites return zero records**? The function completes
  successfully with `datasets_stored = 0`; no events are emitted.
- What happens when the **idempotency store is unavailable**? The function
  fails the invocation rather than risk duplicate writes (fail-closed).
- What happens when a **site ID does not exist** in PVDAQ? The function logs
  a warning for that site and continues processing remaining sites.
- What happens when the **message bus does not exist** at emission time? The
  function fails with a clear error indicating the missing topic/queue.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST execute PVDAQ telemetry retrieval on a configurable
  CRON schedule via a timer trigger.
- **FR-002**: System MUST call the PVDAQ API for each configured site ID and
  retrieve telemetry within a configurable lookback time window.
- **FR-003**: System MUST retry PVDAQ API calls on timeout with exponential
  backoff (max 3 attempts) and respect `Retry-After` headers when
  rate-limited.
- **FR-004**: System MUST write the retrieved CSV data to the bronze storage
  layer at the deterministic path
  `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}.csv`
  using a streaming upload. In local development (`STORAGE_EMULATOR=true`),
  the target MUST be Azurite Blob Storage using the same `azure-storage-blob`
  SDK code path.
- **FR-005**: System MUST compute a SHA-256 hash of the file content during
  streaming upload and store it in the dataset metadata entry.
- **FR-006**: System MUST register dataset metadata in the file tracking store
  after a successful storage write. Metadata MUST include: site_id,
  source_url, storage_path, file_size, ingestion_time (UTC), file_hash
  (SHA-256), and ingestion_id (GUID).
- **FR-007**: System MUST emit one `solar.pvdaq.dataset.available` CloudEvent
  per stored dataset. The event MUST conform to the CloudEvents envelope
  (Principle VII). In local development (`STORAGE_EMULATOR=true`), the event
  MUST be sent to an Azurite Queue instead of Service Bus.
- **FR-008**: System MUST derive a deterministic idempotency key from
  `site_id + date_window` and skip storage write and event emission if the key
  already exists in the idempotency store.
- **FR-009**: System MUST emit structured telemetry per invocation:
  `sites_processed`, `datasets_stored`, `datasets_skipped`, `datasets_failed`,
  `duration_ms`, and `correlation_id`.
- **FR-010**: System MUST continue processing remaining sites when a single
  site's API call or storage write fails (partial-failure resilience).
- **FR-011**: System MUST externalise all configuration: PVDAQ API base URL,
  site ID list, time window, CRON schedule, storage container name, message
  bus topic/queue name, dead-letter queue name, tenant ID. No runtime
  constants may be hardcoded.
- **FR-012**: System MUST NOT parse CSV rows, validate row schema, normalize
  row data, or emit per-row events. These responsibilities belong to the
  downstream processing layer.

### Key Entities

- **PVDAQ Dataset**: A single CSV file downloaded for one site covering one
  polling window. Identified by site_id + date_window. Contains raw
  photovoltaic performance data (unparsed).
- **Dataset Metadata Record**: A file tracking store entry per ingested dataset.
  Fields: site_id, source_url, storage_path, file_size, file_hash (SHA-256),
  ingestion_id (GUID), ingestion_time (UTC).
- **Dataset Event**: A `solar.pvdaq.dataset.available` CloudEvent emitted
  after successful storage. Data block: site_id, storage_path, file_size,
  file_hash, ingestion_id, source_url.
- **Idempotency Record**: A store entry keyed by `site_id + date_window` with
  a TTL (minimum 24 hours) to prevent duplicate downloads and emissions.

## Storage Layout

ADLS Gen2 (prod) / Azurite Blob container `bronze` (local):

```text
raw/pvdaq/
  site_id=9068/
    year=2024/
      month=01/
        pvdaq_9068_2024-01-15.csv
      month=02/
        pvdaq_9068_2024-02-03.csv
  site_id=9069/
    year=2024/
      month=01/
        pvdaq_9069_2024-01-15.csv
```

## Local vs Production Behaviour

| Concern        | Local (`STORAGE_EMULATOR=true`)      | Production                                |
| -------------- | ------------------------------------ | ----------------------------------------- |
| Bronze storage | Azurite Blob port 10000 (`bronze`)   | ADLS Gen2 raw container                   |
| Event emission | Azurite Queue port 10001             | Service Bus topic                         |
| Table storage  | Azurite Table port 10002             | Azure Table Storage                       |
| Auth           | Connection string (devstoreaccount1) | DefaultAzureCredential (managed identity) |

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: PVDAQ CSV files for all configured sites are stored in the bronze
  layer within 60 seconds of the scheduled trigger firing (excluding upstream
  API latency).
- **SC-002**: Each stored dataset produces exactly one `solar.pvdaq.dataset.available`
  event on the configured topic/queue.
- **SC-003**: Duplicate site+date combinations are stored and emitted at most
  once within a 24-hour window.
- **SC-004**: Every invocation produces a complete structured telemetry summary.
- **SC-005**: No raw CSV row parsing, normalization, or per-row event emission
  occurs in this function.
- **SC-006**: Local development works end-to-end using only Azurite (no real
  Azure resources required).

## Assumptions

- The PVDAQ API is accessed via the internal `pvdaq_access` module
  (`src/pvdaq_access.py`) which wraps the NREL Developer API via `httpx`.
- PVDAQ site IDs are known at configuration time. Phase 1 targets 1–10 sites.
- The default lookback window (e.g., previous 24 hours) produces one CSV file
  per site per invocation at the configured granularity.
- `correlation_id` is generated once per function invocation and shared across
  all sites in that batch.

## Constraints

- Row-level parsing, schema validation, normalization, and per-row event
  emission are explicitly out of scope. All such logic belongs to the
  downstream processing layer (feature 003+).
- The function operates within Azure Functions v4 (Python Isolated Worker)
  runtime constraints as declared in `host.json`.
- All timestamps are stored and emitted in UTC.
- No raw payload content containing PII may appear in log output.

## What Moved Downstream

The following responsibilities from spec v1.0 are now in the processing
layer (feature 003+):

| Responsibility              | Old FR   | New Location       |
| --------------------------- | -------- | ------------------ |
| CSV row parsing             | FR-002   | Processing layer   |
| Row-level schema validation | FR-004   | Processing layer   |
| Per-row metadata enrichment | FR-006   | Processing layer   |
| Per-row CloudEvent emission | FR-007   | Processing layer   |
| Per-row dead-lettering      | FR-005   | Processing layer   |
| Per-row idempotency         | FR-008   | Processing layer   |
