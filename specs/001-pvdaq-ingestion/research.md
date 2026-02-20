# Research: PVDAQ Ingestion

**Feature**: 001-pvdaq-ingestion
**Date**: 2026-02-19

## R1: PVDAQ Data Access

**Decision**: Write a thin internal module `pvdaq_access` wrapping the
NREL Developer API directly via `httpx`.

**Rationale**: The `pvdaq_access` library referenced in the spec does
not exist on PyPI. While `pvlib.iotools.get_pvdaq_data()` provides
PVDAQ access, it pulls in the large `pvlib` dependency and does not
give fine-grained control over retry semantics, `Retry-After` handling,
or structured error reporting. A ~200-line internal module wrapping
`httpx` calls to `https://developer.nrel.gov/api/pvdaq/v3/site_data`
provides full control over retry, timeout, and testability via
dependency injection.

**Alternatives considered**:
- `pvlib.iotools.get_pvdaq_data()` — rejected: large dependency, no
  retry control, returns pandas DataFrame (overhead for boundary-only
  ingestion).
- Direct `requests` — rejected: `httpx` is preferred for async support
  and modern Python patterns.

**API details**:
- Base URL: `https://developer.nrel.gov/api/pvdaq/v3/site_data`
- Auth: API key via query param `api_key` or header `X-Api-Key`
- Time-windowed queries: `start_date` + `end_date` parameters
- Rate limits: ~1,000 requests/hour per API key, HTTP 429 with
  `Retry-After` header
- Response: JSON with headers array + data array of records

## R2: Azure Functions Timer Trigger (Python v2 Model)

**Decision**: Use `@app.timer_trigger(schedule="%PVDAQ_CRON_SCHEDULE%")`
with the Python v2 decorator-based programming model.

**Rationale**: The `%AppSetting%` syntax externalises the CRON schedule
per FR-011. The v2 model is the recommended approach for new Python
functions. NCRONTAB 6-field format: `{sec} {min} {hour} {day} {month}
{dow}`. Set `run_on_startup=False` for production.

**Alternatives considered**:
- Durable Functions timer — rejected: unnecessary orchestration
  complexity for a simple polling schedule.

## R3: Service Bus Emission Strategy

**Decision**: Use `azure-servicebus` SDK directly (not output bindings)
for both the primary topic and the dead-letter queue.

**Rationale**: The output binding (`func.Out[str]`) cannot set
`content_type`, `subject`, or application properties on Service Bus
messages. The constitution requires CloudEvents-compatible envelopes
with `content_type="application/cloudevents+json"`. The SDK gives full
control via `ServiceBusMessage(body, content_type, subject,
application_properties)`. Using the SDK for both the primary topic and
the dead-letter queue maintains a single messaging paradigm.

**Alternatives considered**:
- Output binding `@app.service_bus_topic_output()` — rejected: cannot
  set content_type or application properties, insufficient for
  CloudEvents compliance.
- Hybrid (binding for topic, SDK for DLQ) — rejected: inconsistent;
  mixing paradigms increases cognitive load.

**Identity-based connection**:
- App setting: `ServiceBusConnection__fullyQualifiedNamespace` =
  `<namespace>.servicebus.windows.net`
- RBAC role: `Azure Service Bus Data Sender`
- Local dev: `DefaultAzureCredential` falls back to developer identity

## R4: CloudEvents Envelope Construction

**Decision**: Manual dict construction, serialized as JSON, sent with
`content_type="application/cloudevents+json"`.

**Rationale**: Azure Service Bus has no native CloudEvents support
(unlike Event Grid). The CloudEvents envelope structure is simple and
well-defined; a manual dict avoids the `cloudevents` SDK dependency.
Validation is handled by emission contract tests.

**Alternatives considered**:
- CNCF `cloudevents` Python SDK — rejected: adds dependency for a
  simple dict construction; the SDK's value is mostly in HTTP protocol
  binding which does not apply to Service Bus.

## R5: Idempotency Store

**Decision**: Azure Table Storage with `create_entity()` conditional
insert and a periodic cleanup function for TTL enforcement.

**Rationale**: Table Storage provides atomic "insert if not exists"
via `create_entity()` (raises `ResourceExistsError` on duplicate). Point
reads are 1–10ms in the same region. Handles 20,000 TPS per account.
Cost is negligible (~$0.045/GB/month + $0.00036 per 10K transactions).
Durability is guaranteed (replicated storage).

**TTL strategy**: Table Storage has no native TTL. Use a date-based
PartitionKey (e.g., `"2026-02-19"`) and a daily timer-triggered
cleanup function that deletes partitions older than the TTL window.

**Write-before-emit pattern**:
1. `create_entity(Status="pending")` — if `ResourceExistsError`, check
   existing status:
   - `"completed"` → true duplicate, skip
   - `"pending"` → previous crash, retry emit
2. Emit to Service Bus
3. `update_entity(Status="completed")`
4. Enable Service Bus duplicate detection as defense-in-depth

**Alternatives considered**:
- Azure Cache for Redis — rejected: higher cost (~$55/month minimum),
  not durable by default, on retirement path, overkill for idempotency.
- Cosmos DB Table API — rejected: wire-compatible with native TTL, but
  significantly higher cost for a simple idempotency store.

## R6: Secret Management

**Decision**: Key Vault references in app settings
(`@Microsoft.KeyVault(SecretUri=...)`) for the PVDAQ API key.

**Rationale**: Zero-code secret resolution via Managed Identity. The
function reads `os.environ["PVDAQ_API_KEY"]` normally; the platform
resolves from Key Vault transparently. No `azure-keyvault-secrets` SDK
needed in function code.

**Identity-based Service Bus connection** uses
`fullyQualifiedNamespace` (not a secret), so no Key Vault reference
needed for Service Bus.

## R7: host.json Configuration

**Decision**: Configure `functionTimeout`, `logging`, `extensionBundle`,
and Service Bus `clientRetryOptions`.

**Key findings**:
- Timer trigger has **no extension-specific host.json settings**. The
  constitution's mention of `maxPollingInterval` for timer triggers is a
  misnomer — that setting applies to queue-based triggers only. The CRON
  schedule itself governs polling frequency.
- Service Bus `clientRetryOptions` controls retry for output operations.
- `functionTimeout` should accommodate sequential polling of 10 sites
  (~10 minutes max).

## R8: Python Dependencies

**Decision**: Minimal dependency set for boundary-only ingestion.

| Package | Purpose |
|---------|---------|
| `azure-functions` | Function app framework (v2 model) |
| `azure-servicebus` | Service Bus SDK for topic/queue emission |
| `azure-data-tables` | Table Storage SDK for idempotency |
| `azure-identity` | `DefaultAzureCredential` for Managed Identity |
| `httpx` | HTTP client for PVDAQ API calls |
| `jsonschema` | JSON Schema validation (pvdaq-v1.json) |

No additional dependencies required. `cloudevents` SDK explicitly
excluded (manual dict construction preferred).
