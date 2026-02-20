# Feature Specification: PVDAQ Ingestion

**Feature Branch**: `001-pvdaq-ingestion`
**Created**: 2026-02-19
**Status**: Draft
**Input**: User description: "PVDAQ Ingestion — boundary-only ingestion of NREL PVDAQ photovoltaic telemetry"

## Clarifications

### Session 2026-02-19

- Q: Which idempotency key derivation strategy — composite (site ID + timestamp) or SHA-256 hash of payload? → A: Composite key (site ID + timestamp) — deterministic, human-readable, maps to natural PVDAQ record uniqueness.
- Q: Envelope structure — custom envelope or CloudEvents-compatible per constitution? → A: CloudEvents-compatible envelope required. Core fields: `type`, `source`, `id`, `time`, `datacontenttype`. Extension attributes: `tenant_id`, `source_vendor`, `schema_version`, `mapping_version`, `correlation_id`, `ingestion_timestamp`. `data` = original payload unmodified. `mapping_version` = `"unknown"` sentinel in Phase 1 with warning metric.
- Q: Dead-letter destination — built-in Service Bus DLQ, dedicated queue, or blob storage? → A: Dedicated Service Bus queue (e.g., `pvdaq-dead-letter`) — explicit control, independent monitoring and replay.
- Q: Expected data volume per invocation and polling model? → A: Phase 1 targets 1–10 sites, up to 1,000 records per site per poll. Sites polled sequentially to respect NREL rate limits.

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Scheduled Telemetry Retrieval (Priority: P1)

The system retrieves photovoltaic telemetry data from the NREL PVDAQ
dataset on a recurring schedule without manual intervention. An
operations engineer configures a set of PVDAQ site IDs and a polling
schedule; the system then fetches telemetry for those sites within a
configurable lookback window and emits validated raw events into the
event backbone.

**Why this priority**: This is the core value — without scheduled
retrieval, no downstream processing can occur. It is the minimum viable
slice that proves end-to-end data flow from PVDAQ to the event backbone.

**Independent Test**: Can be fully tested by triggering the function
with a mock PVDAQ API response and verifying that a correctly shaped
message appears on the Service Bus topic.

**Acceptance Scenarios**:

1. **Given** a configured CRON schedule and site ID list, **When** the
   timer fires, **Then** the function calls the PVDAQ API for each site
   within the configured time window and emits one raw event per valid
   record to the Service Bus topic.
2. **Given** the PVDAQ API returns zero records for a site, **When** the
   function processes that site, **Then** no events are emitted and a
   structured log entry records "0 records retrieved" for that site.
3. **Given** the PVDAQ API is unreachable, **When** the function
   attempts retrieval, **Then** the function retries with exponential
   backoff (max 3 attempts) and logs a failure with correlation ID
   after exhaustion.

---

### User Story 2 — Schema Validation Gate (Priority: P1)

Every record retrieved from PVDAQ is validated against a versioned JSON
schema before any further processing. Invalid records are rejected at
the boundary, dead-lettered with error metadata, and never propagated
downstream.

**Why this priority**: Tied with P1 because schema validation is
non-negotiable per the constitution — no data may enter the event
backbone without passing validation.

**Independent Test**: Can be tested by submitting a deliberately
malformed payload and verifying it is rejected, dead-lettered, and
logged — while a valid payload passes through.

**Acceptance Scenarios**:

1. **Given** a record that conforms to `pvdaq-v1.json`, **When** the
   function validates it, **Then** validation succeeds and the record
   proceeds to enrichment.
2. **Given** a record with a missing required field, **When** the
   function validates it, **Then** the record is rejected, a structured
   `validation_failure` log entry is emitted, and the record is sent to
   the dead-letter queue with error metadata (field name, violation
   description, correlation ID).
3. **Given** a record with an unexpected data type (e.g., string where
   number expected), **When** the function validates it, **Then** the
   record is rejected with the same dead-letter and logging behaviour.

---

### User Story 3 — Metadata Enrichment & Event Emission (Priority: P1)

Each validated record is enriched with provenance metadata and emitted
as a self-describing raw event to the Service Bus topic. The original
payload is preserved unmodified; metadata is attached in an envelope.

**Why this priority**: Enrichment and emission are inseparable from
retrieval — an event without metadata is untraceable and violates the
constitution.

**Independent Test**: Can be tested by providing a valid PVDAQ record
and asserting that the emitted Service Bus message is a valid
CloudEvents envelope with the correct core fields (`type`, `source`,
`id`, `time`, `datacontenttype`), required extension attributes
(`tenant_id`, `source_vendor`, `schema_version`, `mapping_version`,
`correlation_id`, `ingestion_timestamp`), and the unmodified original
payload under `data`.

**Acceptance Scenarios**:

1. **Given** a validated PVDAQ record, **When** the function enriches
   and emits it, **Then** the emitted event is a CloudEvents-compatible
   message with: `type` = `raw.pvdaq.generation.v1`, `source` =
   `/energy-ingestion-boundary/pvdaq`, `id` = UUID, `time` = UTC
   ISO-8601, `datacontenttype` = `application/json`; extension
   attributes `tenant_id` (from config), `source_vendor` = "PVDAQ",
   `schema_version` = "v1", `mapping_version` = "unknown",
   `correlation_id` = UUID (per invocation), `ingestion_timestamp` =
   UTC ISO-8601; and `data` containing the original payload unmodified.
2. **Given** a Service Bus publish failure, **When** the function
   attempts emission, **Then** it retries with exponential backoff and,
   after exhaustion, fails the invocation so the runtime can surface the
   error.
3. **Given** the `MAPPING_VERSION_PVDAQ` environment variable is not
   set, **When** the function enriches a record, **Then**
   `mapping_version` is set to the sentinel value `"unknown"` and a
   warning metric is emitted.

---

### User Story 4 — Idempotent Emission (Priority: P2)

The function prevents duplicate events from reaching the event backbone
when the same PVDAQ record is encountered more than once (due to
overlapping poll windows, retries, or replayed timer triggers).

**Why this priority**: Idempotency is constitutionally required but
ranks slightly below core retrieval/validation/emission because it
builds on top of a working pipeline.

**Independent Test**: Can be tested by submitting the same PVDAQ record
twice and verifying only one event is emitted; the second invocation
returns success without emission.

**Acceptance Scenarios**:

1. **Given** a PVDAQ record that has not been seen before, **When** the
   function processes it, **Then** the idempotency key is written to the
   store and the event is emitted.
2. **Given** a PVDAQ record whose idempotency key already exists in the
   store, **When** the function processes it, **Then** no event is
   emitted and the function returns success.
3. **Given** an idempotency record older than the configured TTL
   (minimum 24 hours), **When** the same record reappears, **Then** the
   record is treated as new and emitted again.

---

### User Story 5 — Structured Observability (Priority: P2)

Every invocation emits structured telemetry — metrics and logs — so
that operators can monitor ingestion health, detect upstream data
quality regressions, and troubleshoot failures.

**Why this priority**: Observability is constitutionally required and
essential for production readiness, but the pipeline can function (in a
degraded operational state) without it.

**Independent Test**: Can be tested by triggering the function and
asserting that structured log entries and custom metrics appear with the
expected fields and values.

**Acceptance Scenarios**:

1. **Given** a successful invocation, **When** processing completes,
   **Then** structured telemetry is emitted containing: `source` =
   "PVDAQ", `number_of_records_retrieved`, `number_valid`,
   `number_invalid`, `number_emitted`, `duration_ms`, and
   `correlationId`.
2. **Given** an unhandled exception, **When** the function fails,
   **Then** the exception is logged as structured JSON with
   `correlation_id`, `function_name`, and `vendor`, and surfaced as a
   failure status.

---

### Edge Cases

- What happens when the PVDAQ API returns a **partial page** mid-stream
  and then errors? The function continues processing already-retrieved
  records and logs the partial failure.
- What happens when **all records in a batch fail validation**? No
  events are emitted; all records are dead-lettered; observability
  metrics reflect `number_valid = 0`, `number_invalid = N`.
- What happens when the **idempotency store is unavailable**? The
  function fails the invocation rather than risk duplicate emission
  (fail-closed).
- What happens when a configured **site ID does not exist** in PVDAQ?
  The function logs a warning for that site and continues processing
  remaining sites.
- What happens when the **time window configuration yields zero
  records** across all sites? The function completes successfully with
  `number_of_records_retrieved = 0` and emits no events.
- What happens when the **Service Bus topic does not exist** at
  emission time? The function fails with a clear error indicating the
  missing topic.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST execute PVDAQ telemetry retrieval on a
  configurable CRON schedule via a timer trigger.
- **FR-002**: System MUST call the PVDAQ API (via the `pvdaq_access`
  library) for each configured site ID and retrieve telemetry within a
  configurable lookback time window.
- **FR-003**: System MUST retry PVDAQ API calls on timeout with
  exponential backoff (max 3 attempts) and respect `Retry-After`
  headers when rate-limited.
- **FR-004**: System MUST validate every retrieved record against the
  versioned JSON schema `pvdaq-v1.json` as the first processing step
  after deserialization.
- **FR-005**: System MUST reject records that fail schema validation —
  rejected records MUST NOT be emitted to the event backbone, MUST be
  logged as `validation_failure`, and MUST be sent to a dedicated
  dead-letter Service Bus queue (e.g., `pvdaq-dead-letter`) with error
  metadata. The dead-letter queue name MUST be configuration-driven.
- **FR-006**: System MUST emit each valid record as a
  CloudEvents-compatible message. The envelope MUST include:
  - Core fields: `type` (e.g., `raw.pvdaq.generation.v1`), `source`
    (`/energy-ingestion-boundary/pvdaq`), `id` (UUID), `time` (UTC
    ISO-8601), `datacontenttype` (`application/json`).
  - Extension attributes: `tenant_id` (from configuration),
    `source_vendor` = "PVDAQ", `schema_version` matching the validation
    schema, `mapping_version` (read from `MAPPING_VERSION_PVDAQ`
    environment variable or Key Vault; sentinel `"unknown"` if
    unavailable, with warning metric emitted), `correlation_id` (UUID
    generated per invocation), `ingestion_timestamp` (UTC ISO-8601),
    `traceparent` (W3C Trace Context header from invocation context).
  - `data`: the original vendor payload unmodified.
- **FR-007**: System MUST emit validated, enriched events to a
  configuration-driven Service Bus topic (default: `raw-energy-events`).
  The event type MUST follow the naming convention
  `raw.{vendor}.{data_category}.v{major}` and be registered in the
  `topics.md` manifest before first use.
- **FR-008**: System MUST derive a deterministic idempotency key from
  each record's composite identity (site ID + timestamp) and check
  against an idempotency store before emission; duplicates MUST NOT be
  emitted.
- **FR-009**: System MUST emit structured telemetry per invocation:
  `source`, `number_of_records_retrieved`, `number_valid`,
  `number_invalid`, `number_emitted`, `duration_ms`, and
  `correlation_id`.
- **FR-009a**: System MUST propagate distributed trace context by
  including the originating `traceparent` value (W3C Trace Context) as
  a CloudEvents extension attribute on every emitted event. If no
  inbound trace context exists (e.g., timer trigger), the function MUST
  generate a new trace ID for the invocation.
- **FR-009b**: System MUST define configurable alert conditions for:
  validation failure rate spikes (e.g., >10% of records in an
  invocation), emission failures (any Service Bus send error after retry
  exhaustion), and abnormal processing latency (invocation duration
  exceeding a configurable threshold). Alert definitions MUST be
  expressed as infrastructure-as-code (Bicep/Terraform) or documented
  as Application Insights alert rule specifications.
- **FR-010**: System MUST retrieve secrets (API keys) exclusively via
  Managed Identity and Key Vault — no secrets in code, config files, or
  environment variables.
- **FR-011**: System MUST externalise all configuration: PVDAQ API base
  URL, site ID list, time window, CRON schedule, schema version, Service
  Bus topic name, dead-letter queue name, tenant ID, and mapping
  version source (`MAPPING_VERSION_PVDAQ`). No runtime constants may be
  hardcoded.
- **FR-012**: System MUST continue processing remaining records when
  individual records in a batch fail (partial batch failure resilience).

### Key Entities

- **PVDAQ Telemetry Record**: A single measurement row retrieved from
  the PVDAQ API, identified by site ID and timestamp. Contains raw
  photovoltaic performance data (power output, irradiance, temperature,
  etc.).
- **Ingestion Event (CloudEvents)**: A CloudEvents-compatible message
  emitted to Service Bus. Core fields: `type`, `source`, `id`, `time`,
  `datacontenttype`. Extension attributes: `tenant_id`, `source_vendor`,
  `schema_version`, `mapping_version`, `correlation_id`,
  `ingestion_timestamp`. The `data` field contains the unmodified
  original vendor payload.
- **Idempotency Record**: A store entry keyed by a composite of site ID
  + timestamp, with a TTL (minimum 24 hours) used to prevent duplicate
  emission.
- **Dead-Letter Entry**: A rejected record sent to the dedicated
  dead-letter Service Bus queue, containing the original payload,
  validation error details, and correlation metadata.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Valid PVDAQ records are retrievable and emitted to the
  event backbone within 60 seconds of the scheduled trigger firing
  (excluding upstream API latency).
- **SC-002**: 100% of records that fail schema validation are rejected
  and dead-lettered — zero invalid records reach the event backbone.
- **SC-003**: Duplicate records (same site ID + timestamp) are emitted
  at most once within a 24-hour window.
- **SC-004**: Every invocation produces a complete set of structured
  telemetry metrics visible in the observability backend.
- **SC-005**: The function operates without any hardcoded secrets — all
  credentials are resolved at runtime via Managed Identity and Key
  Vault.
- **SC-006**: All configuration values are externally changeable without
  code deployment.
- **SC-007**: A single vendor API failure does not prevent processing of
  records from other configured sites (partial-failure resilience).

## Assumptions

- The PVDAQ ingestion function uses an internal `pvdaq_access` module
  (implemented in `src/pvdaq_access.py`) that wraps the NREL Developer
  API via `httpx` and handles retry, timeout, and `Retry-After` logic.
  No external `pvdaq_access` PyPI package exists.
- PVDAQ site IDs are known at configuration time and do not change
  frequently; they are managed as a list in application configuration.
  Phase 1 targets 1–10 sites.
- Each site returns up to 1,000 records per poll (based on typical
  PVDAQ 1-minute granularity over a 24-hour lookback). Total per
  invocation: up to ~10,000 records.
- Sites are polled sequentially (one API call at a time) to respect
  NREL rate limits. Concurrent polling may be considered in future
  phases if rate limits allow.
- The default lookback time window (e.g., previous 24 hours) is
  sufficient to capture new data without excessive overlap, and is
  tunable per deployment.
- The `tenantId` value "research" is the default for Phase 1 but is
  configuration-driven to support future multi-tenant scenarios.
- `correlationId` is generated once per function invocation and shared
  across all records in that batch, providing a grouping key for
  troubleshooting.
- The PVDAQ API may return records in varying structures across
  different system types; the JSON schema `pvdaq-v1.json` accounts for
  known variations.

## Constraints

- This feature implements boundary-only ingestion. No domain
  normalization, unit conversion, canonical mapping, or transformation
  of any kind occurs at this stage.
- The function operates within Azure Functions v4 (Python Isolated
  Worker) runtime constraints: execution timeout, memory limits, and
  concurrency settings as declared in `host.json`.
- All timestamps are stored and emitted in UTC.
- No raw payload content containing PII may appear in log output.
