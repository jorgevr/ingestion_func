<!--
  Sync Impact Report
  ==================
  Version change: 1.3.0 → 1.4.0
  Modified principles:
    - VIII (Raw Dataset Storage — Bronze Layer): updated deterministic
      path convention to the ratified bronze layer folder structure:
      `source=pvdaq/dataset={site_id}_{category}/ingestion_date=YYYY-MM-DD/{dataset}_v{version}.csv`.
      Added append-only versioning requirement. Supersedes the
      `raw/{vendor}/site_id=…/year=…/month=…` path from v1.3.0.
  Modified sections:
    - Development Workflow / Local Emulation Contract: replaced
      "Azurite Queue (port 10001) / azure-storage-queue" row with
      "Azure Service Bus emulator (Docker) / azure-servicebus".
      Rationale: Azure Service Bus Basic tier does not support topics
      or subscriptions; the official Microsoft Service Bus emulator
      provides queue semantics identical to production. Azurite Queue
      is no longer an acceptable substitute for Service Bus.
    - Ownership Boundaries: updated "Emission to…" bullet to remove
      "Azurite Queue (local)" and replace with "Service Bus emulator
      (Docker) (local)".
  Removed sections: None
  Templates requiring updates:
    - .specify/templates/plan-template.md — ✅ no update needed
    - .specify/templates/spec-template.md — ✅ no update needed
    - .specify/templates/tasks-template.md — ✅ no update needed
  Follow-up TODOs: None
-->

# Energy Ingestion Boundary Constitution

## Purpose

The hardened entry point between the outside world and the event
backbone. This service owns the Azure Functions (Python) that ingest
data from PVDAQ, PVOutput, and future vendor webhooks, download and
store raw datasets to the ADLS Gen2 bronze layer, register dataset
metadata, and emit dataset-level CloudEvents to Event Grid / Service
Bus. Row-level parsing, validation, and transformation are the
responsibility of downstream processing layers.

## Core Principles

### I. Function Isolation

Every Azure Function MUST operate as an independent unit of deployment
and failure.

- Each vendor integration (PVDAQ, PVOutput, webhook receiver) MUST
  reside in its own Function with its own trigger binding.
- Functions MUST NOT share in-process state; all shared context MUST
  flow through bindings, environment variables, or external stores.
- A failure in one Function MUST NOT cascade to another; each Function
  MUST handle its own errors and return appropriate HTTP / trigger
  status codes.
- Functions MUST be independently deployable and independently
  scalable.

**Rationale:** Blast-radius containment — a poison message from one
vendor must never disrupt ingestion of another.

### II. Schema Validation at Boundary

Every payload MUST be validated against a versioned JSON Schema before
any further processing occurs.

- Inbound data (HTTP body, queue message, poll response) MUST be
  validated as the first processing step after deserialization.
- Schemas MUST be versioned (e.g., `pvdaq-v1.json`, `pvoutput-v2.json`)
  and stored in a `schemas/` directory within the repository.
- Validation failures MUST be rejected immediately with a structured
  error response; invalid data MUST NOT propagate downstream.
- Schema changes MUST follow a backwards-compatible evolution strategy
  or introduce a new version.
- Schemas in `schemas/` are the authoritative source; they MUST be
  published as a versioned artifact (e.g., package or CI-produced
  archive) so that downstream consumers can validate against the same
  definitions without duplicating them.
- A `schemas/README.md` MUST document the discovery mechanism (artifact
  feed URL or package reference) and the schema naming convention:
  `{vendor}-v{major}.json` (e.g., `pvdaq-v1.json`).

**Rationale:** The boundary is the single place where external data is
trusted or rejected — garbage must never enter the event backbone.
Publishing schemas as artifacts prevents downstream drift and
duplication.

### III. Metadata Enrichment

Every raw event MUST be enriched with provenance and routing metadata
before emission.

- Enrichment MUST add at minimum: `source_vendor`, `ingestion_timestamp`
  (UTC ISO-8601), `schema_version`, `mapping_version`, and
  `correlation_id`.
- `mapping_version` tags the version of the vendor-to-raw mapping logic
  that produced the event; this service tags the version but MUST NOT
  execute canonical mapping. The authoritative mapping version MUST be
  published by the mapping service as an environment variable
  (`MAPPING_VERSION_{VENDOR}`) or a Key Vault secret; this service
  reads the value at startup and stamps it onto events. If the version
  is unavailable, the Function MUST use the sentinel value `"unknown"`
  and emit a warning metric.
- Enrichment MUST NOT mutate the original payload; metadata MUST be
  attached in an envelope or dedicated metadata block alongside the raw
  data.
- Additional vendor-specific metadata (e.g., site ID, system ID) MUST
  be extracted and surfaced when available.

**Rationale:** Downstream consumers depend on metadata for routing,
replay, and audit — events without provenance are untraceable.

### IV. Managed Identity

All authentication to Azure services MUST use Managed Identity; no
secrets in code or configuration.

- Functions MUST authenticate to Event Grid, Service Bus, Key Vault,
  and any other Azure resource using System-Assigned or User-Assigned
  Managed Identity.
- Connection strings, API keys, and SAS tokens MUST NOT appear in
  application settings, source code, or environment variables; secrets
  required for third-party vendor APIs MUST be retrieved from Key Vault
  at runtime via Managed Identity.
- Local development MUST use `DefaultAzureCredential` to fall back to
  developer identity without code changes.
- Infrastructure-as-code MUST provision RBAC role assignments; no
  shared-key access policies.

**Rationale:** Secrets in config are the most common breach vector for
cloud functions — Managed Identity eliminates this class of risk.

### V. Structured Observability

All logging, tracing, and metrics MUST be structured, correlated, and
emitted to Azure Monitor / Application Insights.

- Log entries MUST be structured JSON (no free-text print statements);
  every entry MUST include `correlation_id`, `function_name`, and
  `vendor`.
- Functions MUST emit custom metrics for: events received, events
  validated, events rejected, events emitted, and processing latency.
- Distributed tracing MUST propagate `traceparent` headers where
  applicable; all emitted events MUST carry the originating trace ID.
- Alerts MUST be configurable on: validation failure spikes, emission
  failures, and abnormal latency.

**Rationale:** The boundary is the first place to detect upstream data
quality issues and the last place to detect emission failures —
observability here protects the entire pipeline.

### VI. Idempotency

Every ingestion path MUST guarantee effective exactly-once semantics
for event emission within the boundary, achieved through at-least-once
delivery combined with idempotent deduplication.

- Each inbound request or polled record MUST be assigned a
  deterministic idempotency key derived from the payload content (e.g.,
  vendor + site + timestamp hash), not from transport-level IDs.
- The idempotency check and event emission MUST be performed as an
  atomic operation: the Function MUST write the idempotency record and
  emit the event within a single transactional scope (e.g., conditional
  insert to Table Storage followed by output binding, with rollback on
  failure). If true atomicity is not achievable, the Function MUST
  write the idempotency record *before* emission and accept that a
  crash between write and emit may require manual replay from
  dead-letter inspection — preferring at-most-once over duplication.
- Idempotency records MUST include a TTL appropriate to the vendor's
  delivery semantics (minimum 24 hours for poll-based sources).
- Idempotency MUST be enforced at the boundary — downstream services
  MUST NOT be relied upon for deduplication of raw events.

**Rationale:** Vendor APIs retry, webhooks replay, and polls overlap —
the boundary must absorb duplicates so the event backbone never sees
them. True distributed exactly-once is impractical; write-before-emit
with dead-letter recovery provides the best practical guarantee.

### VII. Event Emission Rules

All events emitted by this service MUST target Event Grid or Service
Bus (production) or Azurite Queue (local development) as immutable,
self-describing messages.

- Emitted events MUST use a CloudEvents-compatible envelope with
  `type`, `source`, `id`, `time`, and `datacontenttype` fields.
- The `data` payload MUST describe the ingested dataset (storage path,
  file hash, site ID, etc.); enrichment metadata MUST reside in
  CloudEvents extension attributes or a dedicated metadata block.
- Emission MUST target a single, well-known topic or queue per event
  type; routing logic MUST NOT be embedded in the Function beyond
  topic selection. The canonical event type for this service is the
  domain-level dataset lifecycle convention:
  `{domain}.{vendor}.{entity}.{action}` (e.g.,
  `solar.pvdaq.dataset.available`). Per-row raw data event types
  (`raw.{vendor}.{data_category}.v{major}`) are reserved for future
  downstream layers, not this boundary service. A new event type MUST
  be registered in a `topics.md` manifest before first use.
- Failed emissions MUST be retried with exponential backoff; after
  exhausting retries, the event MUST be routed to a dead-letter
  destination and an alert MUST fire.
- Events are immutable once emitted — no downstream process may
  request the boundary to mutate or delete a previously emitted event.

**Rationale:** The event backbone depends on a predictable, immutable
stream of dataset-level signals — the boundary's role is to store raw
files and announce their availability, not to parse or transform rows.

### VIII. Raw Dataset Storage — Bronze Layer

All downloaded vendor data MUST be persisted to the ADLS Gen2 raw
container (bronze layer) before any event is emitted.

- Files MUST be written using streaming/chunked uploads so that memory
  usage remains bounded regardless of file size.
- Files MUST be stored at a deterministic, partition-friendly path following
  the bronze layer folder convention:
  `source={vendor}/dataset={site_id}_{category}/ingestion_date={YYYY-MM-DD}/{dataset}_v{version}.csv`
  (e.g., `source=pvdaq/dataset=9068_ac_power/ingestion_date=2024-01-15/9068_ac_power_v1.csv`).
  `ingestion_date` is the UTC calendar date of ingestion, NOT the source
  file's last-modified date. `version` is an incrementing integer starting
  at 1; each ingestion of the same logical dataset creates a new version.
- Storage MUST be append-only. Files MUST NOT be overwritten or deleted;
  re-ingestion of a changed source file creates a new version.
- A SHA-256 file hash MUST be computed during streaming upload and
  stored in dataset metadata for downstream integrity verification.
- Event emission MUST NOT occur until the file write has been confirmed
  by the storage layer (write-before-emit).
- In local development, Azurite Blob Storage MUST be used in place of
  ADLS Gen2. The same Azure Blob Storage SDK code path MUST be used
  for both environments — the only difference is the endpoint and
  credentials (see Local Emulation Contract in Development Workflow).

**Rationale:** Storing raw files in a durable bronze layer decouples
ingestion latency from downstream processing capacity and provides a
replay source if downstream failures occur.

## Ownership Boundaries

### This Service Owns

- Azure Functions (Python) for all vendor integrations
- Webhook receivers and vendor polling schedules
- Streaming download of raw vendor files
- ADLS Gen2 bronze writes (streaming upload, path management, blob client)
- Dataset metadata registration (file tracking store)
- File-level idempotency — skip files already successfully ingested
- Dataset-level CloudEvent emission (`solar.pvdaq.dataset.available`)
- Emission to Service Bus queue (prod) or Service Bus emulator queue (local)
- Observability at the ingestion boundary

### This Service Does NOT Own

- Row-level CSV parsing, schema validation, or normalization
- Per-row event emission or per-row dead-lettering
- Canonical data model or unit conversion
- Mapping execution (vendor-to-canonical transformation)
- AI/ML inference logic
- Databricks pipelines or downstream processing
- Event routing rules beyond topic/queue selection

## Development Workflow

- All changes MUST pass schema validation unit tests and emission
  contract tests before merge. Emission contract tests MUST verify:
  (a) the emitted message conforms to the CloudEvents envelope schema,
  (b) required extension attributes (`source_vendor`, `schema_version`,
  `correlation_id`) are present, (c) the target topic matches the
  event-type naming convention, and (d) dead-letter routing activates
  after retry exhaustion.
- New vendor integrations MUST include: a JSON Schema, an integration
  test with sample payloads, and observability instrumentation.
- Infrastructure changes MUST be expressed as code (Bicep / Terraform)
  and reviewed alongside application changes.
- Each Function MUST declare its concurrency and scaling limits in
  `host.json` or per-function configuration. At minimum:
  `maxConcurrentRequests` (HTTP triggers), `batchSize` and
  `maxBatchSize` (queue triggers), and `maxPollingInterval` (timer
  triggers) MUST be tuned per vendor to respect upstream rate limits
  and avoid self-inflicted throttling. Default values MUST NOT be used
  without explicit justification in the PR description.
- Every PR MUST demonstrate that the 8 Core Principles are upheld; the
  plan's Constitution Check gate enforces this.

### Local Emulation Contract

All local development MUST emulate Azure cloud services using Azurite
so that no real Azure resources are required to run or test the service
locally. The contract is:

| Production service      | Local emulator                            | SDK used               |
| ----------------------- | ----------------------------------------- | ---------------------- |
| ADLS Gen2 (bronze)      | Azurite Blob (port 10000)                 | `azure-storage-blob`   |
| Service Bus (queue)     | Azure Service Bus emulator (Docker)       | `azure-servicebus`     |
| Azure Table Storage     | Azurite Table (port 10002)                | `azure-data-tables`    |

> **Note**: Azure Service Bus Basic tier is in use — topics and subscriptions
> are not available. All inter-service communication uses queues. Azurite Queue
> (`azure-storage-queue`) is **not** a substitute for Service Bus; the official
> Microsoft Service Bus emulator MUST be used for local development. A
> `docker-compose.yml` at the service root MUST define the Service Bus emulator
> container alongside Azurite so that `docker compose up` provides a complete
> local dev environment with no cloud dependency.

- Code MUST use the same Azure SDK interfaces for both environments.
  Environment selection MUST be controlled by a single environment
  variable (`STORAGE_EMULATOR=true` or presence of a connection string
  pointing to Azurite).
- Azurite endpoints MUST be set via the well-known
  `AzureWebJobsStorage` connection string (Azurite devstoreaccount1).
- Blob container names, queue names, and table names MUST be identical
  in both environments. The only difference is the storage endpoint
  and credential resolver.
- Integration tests MUST run against the Azurite emulator (no real
  Azure resources) and MUST be deterministic and repeatable without
  network access.

## Governance

This constitution supersedes all other development practices for the
energy-ingestion-boundary repository. Amendments require:

1. A written proposal documenting the change and its rationale.
2. Review and approval by the repository owner. MAJOR amendments
   (principle removal or redefinition) MUST additionally be approved by
   at least one other maintainer or technical lead.
3. A migration plan if the amendment affects existing Functions or
   emitted event schemas.
4. Version bump following semantic versioning (MAJOR for principle
   removal/redefinition, MINOR for new principles or material
   expansion, PATCH for clarifications).

All pull requests and code reviews MUST verify compliance with these
principles. Violations MUST be resolved before merge.

**Version**: 1.4.0 | **Ratified**: 2026-02-19 | **Last Amended**: 2026-04-15
