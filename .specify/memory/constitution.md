<!--
  Sync Impact Report
  ==================
  Version change: 1.1.0 → 1.2.0
  Modified principles:
    - II (Schema Validation at Boundary): added schema artifact publishing
      and discovery guidance, schema naming convention
    - III (Metadata Enrichment): clarified mapping_version source mechanism
      (env var / Key Vault, "unknown" sentinel fallback)
    - VI (Idempotency): reframed as effective exactly-once via dedup;
      specified atomic check-and-emit requirement with write-before-emit
      fallback
    - VII (Event Emission Rules): added event-type naming convention
      (raw.{vendor}.{data_category}.v{major}) and topics.md manifest;
      expanded in 1.2.0 to add domain-level lifecycle event convention
      ({domain}.{vendor}.{entity}.{action}) for dataset/notification events
  Added sections / clauses:
    - Development Workflow: concurrency/scaling limits requirement
      (host.json tuning per vendor)
    - Development Workflow: emission contract test definition (CloudEvents
      envelope, extension attributes, topic naming, dead-letter routing)
    - Governance: MAJOR amendment quorum (second reviewer required)
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
data from PVDAQ, PVOutput, and future vendor webhooks, validate it,
enrich it with metadata, and emit raw events to Event Grid / Service
Bus.

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

All raw events MUST be emitted to Event Grid or Service Bus as
immutable, self-describing messages.

- Emitted events MUST use a CloudEvents-compatible envelope with
  `type`, `source`, `id`, `time`, and `datacontenttype` fields.
- The `data` payload MUST contain the original vendor data unmodified;
  enrichment metadata MUST reside in CloudEvents extension attributes
  or a dedicated metadata block.
- Emission MUST target a single, well-known topic or queue per event
  type; routing logic MUST NOT be embedded in the Function beyond
  topic selection. Event types MUST follow one of these naming
  conventions: (a) `raw.{vendor}.{data_category}.v{major}` for
  per-record raw data events (e.g., `raw.pvdaq.generation.v1`), or
  (b) `{domain}.{vendor}.{entity}.{action}` for domain-level
  lifecycle events (e.g., `solar.pvdaq.dataset.available`). A new
  event type MUST be registered in a `topics.md` manifest before
  first use.
- Failed emissions MUST be retried with exponential backoff; after
  exhausting retries, the event MUST be routed to a dead-letter
  destination and an alert MUST fire.
- Events are immutable once emitted — no downstream process may
  request the boundary to mutate or delete a previously emitted event.

**Rationale:** The event backbone depends on a predictable, immutable
stream of raw events — the boundary's contract is to emit clean,
self-describing messages and never silently drop data.

## Ownership Boundaries

### This Service Owns

- Azure Functions (Python) for all vendor integrations
- Webhook receivers and vendor polling schedules
- Schema definitions and validation logic
- Metadata enrichment and mapping version tagging
- Emission to Event Grid / Service Bus
- Observability at the ingestion boundary
- Idempotency store and deduplication logic

### This Service Does NOT Own

- Canonical data model or unit normalization
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
- Every PR MUST demonstrate that the 7 Core Principles are upheld; the
  plan's Constitution Check gate enforces this.

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

**Version**: 1.2.0 | **Ratified**: 2026-02-19 | **Last Amended**: 2026-03-09
