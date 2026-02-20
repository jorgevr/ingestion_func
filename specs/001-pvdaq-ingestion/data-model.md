# Data Model: PVDAQ Ingestion

**Feature**: 001-pvdaq-ingestion
**Date**: 2026-02-19
**Source**: [spec.md](spec.md), [research.md](research.md)

## Entities

### 1. PVDAQ Telemetry Record (Inbound)

A single measurement row retrieved from the NREL PVDAQ API. This is the raw
inbound payload — validated but never mutated.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `SiteID` | integer | yes | PVDAQ site identifier |
| `measdatetime` | string (ISO-8601) | yes | Measurement timestamp (UTC) |
| `ac_power` | number | no | AC power output (W) |
| `dc_power` | number | no | DC power output (W) |
| `poa_irradiance` | number | no | Plane-of-array irradiance (W/m²) |
| `ambient_temp` | number | no | Ambient temperature (°C) |
| `module_temp` | number | no | Module temperature (°C) |
| `wind_speed` | number | no | Wind speed (m/s) |
| `inverter_efficiency` | number | no | Inverter efficiency (%) |

**Notes**:
- Field set varies by PVDAQ system type; schema `pvdaq-v1.json` uses
  `additionalProperties: true` to accommodate vendor-specific fields.
- `SiteID` + `measdatetime` form the natural composite key (uniqueness constraint).
- Original PVDAQ API response wraps records in `{"headers": [...], "data": [[...]]}`;
  the `pvdaq_access` module transforms this into flat dicts keyed by header names.

**Validation**: `schemas/pvdaq-v1.json` — requires `SiteID` (integer) and
`measdatetime` (string, ISO-8601 pattern). All other fields are optional with
type constraints.

---

### 2. Ingestion Event (CloudEvents Envelope)

The emitted message on the Service Bus topic. Wraps the unmodified PVDAQ record
in a CloudEvents-compatible envelope.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `specversion` | string | yes | Always `"1.0"` |
| `type` | string | yes | `"raw.pvdaq.generation.v1"` |
| `source` | string | yes | `"/energy-ingestion-boundary/pvdaq"` |
| `id` | string (UUID) | yes | Unique event ID |
| `time` | string (ISO-8601) | yes | Event emission timestamp (UTC) |
| `datacontenttype` | string | yes | `"application/json"` |
| `tenant_id` | string | yes | From configuration (default: `"research"`) |
| `source_vendor` | string | yes | `"PVDAQ"` |
| `schema_version` | string | yes | `"v1"` (matches validation schema) |
| `mapping_version` | string | yes | From `MAPPING_VERSION_PVDAQ` env var; `"unknown"` sentinel if unavailable |
| `correlation_id` | string (UUID) | yes | Generated once per function invocation |
| `ingestion_timestamp` | string (ISO-8601) | yes | Time of enrichment (UTC) |
| `data` | object | yes | Original PVDAQ telemetry record (unmodified) |

**Service Bus message properties**:
- `content_type`: `"application/cloudevents+json"`
- `subject`: `"raw.pvdaq.generation.v1"`
- `application_properties`: `{"source_vendor": "PVDAQ", "schema_version": "v1"}`

---

### 3. Idempotency Record (Azure Table Storage)

Tracks which records have been processed to prevent duplicate emission.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `PartitionKey` | string | yes | Date-based partition: `"YYYY-MM-DD"` (from `measdatetime`) |
| `RowKey` | string | yes | Composite key: `"{SiteID}_{measdatetime_iso}"` |
| `Status` | string | yes | `"pending"` or `"completed"` |
| `CorrelationId` | string | yes | Invocation correlation ID |
| `CreatedAt` | string (ISO-8601) | yes | Record creation timestamp (UTC) |

**State transitions**:
```
[new record] → create_entity(Status="pending")
                    │
                    ├── emit succeeds → update_entity(Status="completed")
                    │
                    └── emit fails → Status remains "pending"
                                     (next invocation retries emit)
```

**Duplicate detection**:
- `create_entity()` raises `ResourceExistsError` on duplicate key
- If existing record has `Status="completed"` → true duplicate, skip
- If existing record has `Status="pending"` → previous crash, retry emit

**TTL enforcement**:
- Date-based `PartitionKey` enables efficient partition-level cleanup
- A separate daily timer function deletes partitions older than the TTL window (minimum 24 hours)

---

### 4. Dead-Letter Entry

A rejected record sent to the dedicated dead-letter Service Bus queue.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `original_payload` | object | yes | The raw PVDAQ record that failed validation |
| `error_type` | string | yes | `"validation_failure"` |
| `error_details` | array | yes | List of validation errors (field, message, path) |
| `correlation_id` | string (UUID) | yes | Invocation correlation ID |
| `site_id` | integer | yes | PVDAQ site ID (extracted for debugging) |
| `timestamp` | string (ISO-8601) | yes | Time of rejection (UTC) |
| `source_vendor` | string | yes | `"PVDAQ"` |
| `schema_version` | string | yes | Schema that was used for validation |

**Service Bus message properties**:
- Queue: configuration-driven (default: `pvdaq-dead-letter`)
- `content_type`: `"application/json"`
- `subject`: `"validation_failure"`

## Relationships

```
PVDAQ API ──[poll]──► PVDAQ Telemetry Record
                            │
                     [validate against pvdaq-v1.json]
                            │
                     ┌──────┴──────┐
                     │             │
                  [valid]      [invalid]
                     │             │
              [check idemp.]  [Dead-Letter Entry]
                     │             │
              ┌──────┴──────┐     ▼
              │             │  pvdaq-dead-letter queue
           [new]       [duplicate]
              │             │
    [Ingestion Event]   [skip, log]
              │
              ▼
    raw-energy-events topic
```

## Configuration Entities

All configuration is externalized per FR-011.

| Setting | App Setting Key | Default | Description |
|---------|----------------|---------|-------------|
| PVDAQ API Base URL | `PVDAQ_API_BASE_URL` | `https://developer.nrel.gov/api/pvdaq/v3` | NREL API endpoint |
| PVDAQ API Key | `PVDAQ_API_KEY` | (Key Vault reference) | `@Microsoft.KeyVault(SecretUri=...)` |
| Site ID List | `PVDAQ_SITE_IDS` | — | Comma-separated list of integer site IDs |
| Lookback Window (hours) | `PVDAQ_LOOKBACK_HOURS` | `24` | Time window for each poll |
| CRON Schedule | `PVDAQ_CRON_SCHEDULE` | `0 0 */6 * * *` | NCRONTAB 6-field format |
| Service Bus Namespace | `ServiceBusConnection__fullyQualifiedNamespace` | — | Identity-based connection |
| Topic Name | `SERVICE_BUS_TOPIC_NAME` | `raw-energy-events` | Emission target topic |
| Dead-Letter Queue | `DEAD_LETTER_QUEUE_NAME` | `pvdaq-dead-letter` | Rejection target queue |
| Idempotency Table | `IDEMPOTENCY_TABLE_NAME` | `pvdaqidempotency` | Table Storage table name |
| Table Storage Connection | `TableStorageConnection__tableServiceUri` | — | Identity-based connection |
| Tenant ID | `TENANT_ID` | `research` | CloudEvents `tenant_id` |
| Mapping Version | `MAPPING_VERSION_PVDAQ` | `unknown` | CloudEvents `mapping_version` |
| Schema Version | `SCHEMA_VERSION_PVDAQ` | `v1` | Validation schema version |
