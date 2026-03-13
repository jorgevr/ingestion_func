# Quickstart: PVDAQ Historical Dataset Ingestion

## Prerequisites

- Python 3.11+
- Azure Functions Core Tools v4
- Azurite (local storage emulator) for Table Storage
- Access to Azure Service Bus namespace (or local emulator)
- Access to ADLS Gen2 storage account (or Azurite with HNS)

## Setup

1. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

2. **Configure local settings** — copy template and fill in values:

   ```bash
   cp local.settings.json.template local.settings.json
   ```

   Key settings for feature 002:

   ```json
   {
     "PVDAQ_HISTORICAL_SITE_IDS": "9068,9069,2107,7333",
     "PVDAQ_HISTORICAL_CRON_SCHEDULE": "0 0 */6 * * *",
     "PVDAQ_HISTORICAL_QUEUE_NAME": "pvdaq-historical-work",
     "FILE_TRACKING_TABLE_NAME": "PvdaqFileTracking",
     "OEDI_BUCKET_URL": "https://oedi-data-lake.s3.amazonaws.com",
     "OEDI_HISTORICAL_PREFIX": "pvdaq/2023-solar-data-prize",
     "ADLS_ACCOUNT_URL": "https://{account}.dfs.core.windows.net",
     "ADLS_CONTAINER_NAME": "raw"
   }
   ```

3. **Start Azurite** (for Table Storage):

   ```bash
   azurite --silent --location ./AzuriteConfig --debug ./AzuriteConfig/debug.log
   ```

4. **Run the function app locally**:

   ```bash
   func start
   ```

## Architecture Overview

```text
Timer Trigger (every 6 hours)
  │
  ▼
historical_dispatcher()
  ├── For each site (9068, 9069, 2107, 7333):
  │     ├── List CSV files via S3 ListObjectsV2
  │     ├── Check file tracking table for new/changed files
  │     └── Enqueue work item per unprocessed file
  │
  ▼
Service Bus Queue: pvdaq-historical-work
  │
  ▼
historical_worker() (one invocation per file)
  ├── Stream download from S3 (httpx async)
  ├── Stream upload to ADLS Gen2 (append + flush)
  ├── Compute SHA-256 hash during streaming (one-pass)
  ├── Update file tracking entity with metadata
  │     (storage_path, file_hash, ingestion_id, source_url)
  ├── Emit solar.pvdaq.dataset.available CloudEvent
  └── Mark file as completed in tracking table
```

## Running Tests

```bash
# All tests
pytest

# Only feature 002 tests
pytest tests/unit/test_oedi_historical_client.py tests/unit/test_adls_store.py tests/integration/test_historical_pipeline.py

# With coverage
pytest --cov=src --cov-report=term-missing
```

## Key Modules

| Module                           | Purpose                                               |
| -------------------------------- | ----------------------------------------------------- |
| `src/oedi_historical_client.py`  | S3 listing + streaming CSV download                   |
| `src/adls_store.py`             | ADLS Gen2 streaming upload with SHA-256 hash (NEW)    |
| `src/config.py`                 | Extended with ADLS config fields                      |
| `src/file_tracking_store.py`    | Extended with dataset metadata fields                 |
| `src/cloudevents_envelope.py`   | Extended for dataset-level event builder              |
| `function_app.py`               | `historical_dispatcher` + `historical_worker`         |

## Shared Modules (from feature 001, unchanged)

| Module                          | Purpose                          |
| ------------------------------- | -------------------------------- |
| `src/service_bus_emitter.py`    | Service Bus topic/queue sender   |
| `src/observability.py`          | Structured logging, metrics      |
| `src/http_retry.py`            | HTTP retry with exponential backoff |

## Modules No Longer Used by Feature 002

| Module                          | Reason                                        |
| ------------------------------- | --------------------------------------------- |
| `src/csv_normalizer.py`        | Row parsing moved to downstream processing    |
| `src/record_pipeline.py`       | Per-row pipeline moved downstream             |
| `src/schema_validator.py`      | Row validation moved downstream               |
| `src/idempotency_store.py`     | Per-row idempotency replaced by file tracking |
