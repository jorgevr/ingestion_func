# PVDAQ Ingestion Function

Boundary-only ingestion of NREL PVDAQ photovoltaic telemetry into the energy-ingestion-boundary event backbone.

Azure Functions v4 (Python Isolated Worker, v2 programming model). Fan-out
architecture (timer-triggered dispatcher + queue-triggered worker) for bulk
historical CSV ingestion from the OEDI 2023 Solar Data Prize dataset: streams
each CSV from S3 straight to ADLS Gen2 bronze and emits a dataset-level
CloudEvent per file.

> Feature 001 (daily polling via a `pvdaq_ingestion` timer function, with
> per-record validation/idempotency/emission) was retired in R2.9: the
> PVDAQ source only holds historical data and nothing downstream consumed
> the daily path.

## Data Source

**OEDI Data Lake** — public S3 bucket at `https://oedi-data-lake.s3.amazonaws.com/pvdaq/2023-solar-data-prize/`

- Historical CSV files per site, discovered via S3 `ListObjectsV2`
- No authentication required (public bucket)

## Source Modules

| Module | Purpose |
|--------|---------|
| `function_app.py` | Timer/queue triggers + historical dispatcher/worker orchestration |
| `src/config.py` | Externalized configuration loader (all settings from env vars) |
| `src/observability.py` | Structured JSON logging, `DatasetIngestionStats` dataclass, metrics emission |
| `src/cloudevents_envelope.py` | CloudEvents v1.0 dataset-available envelope builder (contract-validated) |
| `src/service_bus_emitter.py` | Service Bus SDK sender (queue + dead-letter queue) |
| `src/oedi_historical_client.py` | OEDI S3 historical client (ListObjectsV2 XML, streaming CSV download) |
| `src/file_tracking_store.py` | Azure Table Storage file tracking (incremental detection, status lifecycle) |
| `src/adls_store.py` | ADLS Gen2 bronze writes (streaming upload, Blob API) |

## Pipeline Flow

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
Parse + validate work item against work-item.v1.json
    |     +-- Invalid/malformed → Dead-letter
    |
    v
Mark file as "processing" → Stream S3 → ADLS Gen2 (chunked, bounded memory)
    |
    +-> Write metadata.json sidecar (validated against metadata-file.v1.json)
    +-> Build + validate dataset-available CloudEvent → Emit
    |     +-- Any validation failure → Dead-letter (distinct reason code)
    |
    v
Mark file as "completed" in Table Storage (LAST — after the event is published)
Emit DatasetIngestionStats telemetry
```

## Infrastructure

| File | Purpose |
|------|---------|
| `host.json` | Azure Functions config: 10min timeout, Service Bus retry (exponential, 3 retries) |
| `requirements.txt` | Pinned production dependencies |
| `requirements-dev.txt` | Dev dependencies (pytest, pytest-asyncio, respx, ruff) |
| `schemas/work-item.v1.json` | Dispatcher → worker queue message schema |
| `topics.md` | Event type manifest (`solar.pvdaq.dataset.available.v1`) |
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
