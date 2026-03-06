# PVDAQ Ingestion Function

Boundary-only ingestion of NREL PVDAQ photovoltaic telemetry into the energy-ingestion-boundary event backbone.

Azure Functions v4 (Python Isolated Worker, v2 programming model) that retrieves telemetry from the OEDI Data Lake (public S3 bucket), validates records against a versioned JSON schema, enriches them with CloudEvents-compatible metadata, and emits them to an Azure Service Bus topic.

Two ingestion modes:

- **Feature 001 — Daily polling**: Timer-triggered function polls daily CSV files for configured sites
- **Feature 002 — Historical bulk download**: Fan-out architecture with timer-triggered dispatcher + queue-triggered worker for bulk historical CSV ingestion from the OEDI 2023 Solar Data Prize dataset

## Data Source

**OEDI Data Lake** — public S3 bucket at `https://oedi-data-lake.s3.amazonaws.com/pvdaq/csv/`

- Systems inventory: `systems_20250729.csv`
- Daily telemetry: `pvdata/system_id={id}/year={Y}/month={M}/day={D}/system_{id}__date_{Y}_{MM}_{DD}.csv`
- 5-minute measurement intervals, CSV format
- No authentication required (public bucket)

Records are normalized on ingestion: `system_id` → `SiteID`, `measured_on` → `measdatetime`, sensor suffixes stripped (`dc_power__346` → `dc_power`).

## Source Modules

| Module | Purpose |
|--------|---------|
| `function_app.py` | Timer trigger + full pipeline orchestration |
| `src/config.py` | Externalized configuration loader (all settings from env vars) |
| `src/observability.py` | Structured JSON logging, `InvocationStats` dataclass, metrics emission |
| `src/oedi_data_lake.py` | OEDI S3 Data Lake client (httpx) with retry/backoff and CSV normalization |
| `src/schema_validator.py` | JSON Schema validation gate (`pvdaq-v1.json`, Draft202012Validator) |
| `src/cloudevents_envelope.py` | CloudEvents v1.0 envelope builder with extension attributes |
| `src/service_bus_emitter.py` | Service Bus SDK sender (topic + dead-letter queue) |
| `src/idempotency_store.py` | Azure Table Storage idempotency (write-before-emit pattern, TTL cleanup) |
| `src/csv_normalizer.py` | Historical CSV record normalizer (sensor suffix stripping, timestamp detection) |
| `src/oedi_historical_client.py` | OEDI S3 historical client (ListObjectsV2 XML, streaming CSV download) |
| `src/file_tracking_store.py` | Azure Table Storage file tracking (incremental detection, status lifecycle) |

## Test Coverage

| Category | Files | Tests |
|----------|-------|-------|
| Unit: config | `tests/unit/test_config.py` | 20 |
| Unit: schema validation | `tests/unit/test_schema_validation.py` | 6 |
| Unit: metadata enrichment | `tests/unit/test_metadata_enrichment.py` | 7 |
| Unit: OEDI data lake | `tests/unit/test_oedi_data_lake.py` | 13 |
| Unit: idempotency | `tests/unit/test_idempotency.py` | 7 |
| Unit: observability | `tests/unit/test_observability.py` | 6 |
| Contract: CloudEvents envelope | `tests/contract/test_cloudevents_envelope.py` | 7 |
| Contract: invalid payload | `tests/contract/test_invalid_payload.py` | 4 |
| Contract: valid payload | `tests/contract/test_valid_payload.py` | 2 |
| Integration: dead-letter | `tests/integration/test_dead_letter.py` | 1 |
| Integration: OEDI data lake | `tests/integration/test_oedi_integration.py` | 3 |
| Integration: Service Bus emission | `tests/integration/test_service_bus_emission.py` | 3 |
| Integration: idempotency | `tests/integration/test_idempotency_integration.py` | 3 |

## Pipeline Flow

```
Timer Trigger (CRON)
    |
    v
Load Config + Generate correlation_id + traceparent
    |
    v
Resolve site IDs (explicit list or discover from OEDI systems CSV)
    |
    v
For each site_id (sequential):
    |
    +-> For each date in lookback window:
         |
         +-> Fetch daily CSV from OEDI Data Lake (httpx, retry/backoff)
         |
         +-> For each record (normalized from CSV):
              |
              +-> Validate against pvdaq-v1.json
              |     |
              |     +-- Invalid -> Dead-letter queue + log
              |     |
              |     +-- Valid -+
              |               |
              |               v
              |         Idempotency check (Table Storage)
              |               |
              |               +-- Duplicate -> Skip + log
              |               |
              |               +-- New/Retry -+
              |                              |
              |                              v
              |                    Build CloudEvents envelope
              |                              |
              |                              v
              |                    Emit to Service Bus topic
              |                              |
              |                              v
              |                    Mark idempotency completed
              |
              v
Emit InvocationStats telemetry
```

### Feature 002 — Historical Bulk Download (Fan-Out)

```text
Timer Trigger (CRON) — historical_dispatcher
    |
    v
For each configured site_id:
    |
    +-> List CSV files via S3 ListObjectsV2
    +-> Filter to new/changed files (FileTrackingStore)
    +-> Enqueue work item per file → Service Bus queue
    +-> Mark file as "queued" in Table Storage

Queue Trigger — historical_worker (1 file per invocation)
    |
    v
Stream CSV rows (chunked, bounded memory)
    |
    +-> Normalize record (inject SiteID, strip sensor suffixes)
    +-> Validate against pvdaq-v1.json
    |     +-- Invalid → Dead-letter
    |     +-- Valid → Idempotency check → Emit CloudEvents → Mark completed
    |
    v
Mark file as "completed" in Table Storage
Emit InvocationStats telemetry
```

## Infrastructure

| File | Purpose |
|------|---------|
| `host.json` | Azure Functions config: 10min timeout, Service Bus retry (exponential, 3 retries) |
| `requirements.txt` | Pinned production dependencies |
| `requirements-dev.txt` | Dev dependencies (pytest, pytest-asyncio, respx, ruff) |
| `schemas/pvdaq-v1.json` | Authoritative PVDAQ validation schema |
| `alerts/pvdaq-alerts.bicep` | Application Insights alert rules |
| `topics.md` | Event type manifest (`raw.pvdaq.generation.v1`) |
| `local.settings.json.template` | All configuration settings with placeholders |

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Copy and configure local settings
cp local.settings.json.template local.settings.json
# Edit local.settings.json with your values

# Run tests
pytest

# Lint
ruff check .

# Start function locally
func start
```
