# Quickstart: PVDAQ Historical Data Ingestion

## Prerequisites

- Python 3.11+
- Azure Functions Core Tools v4
- Azurite (local storage emulator) for Table Storage
- Access to Azure Service Bus namespace (or local emulator)

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
     "OEDI_HISTORICAL_PREFIX": "pvdaq/2023-solar-data-prize"
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
  ├── Download CSV via streaming HTTP GET
  ├── Parse rows incrementally (bounded memory)
  ├── For each row:
  │     ├── Normalize (inject SiteID, strip suffixes, detect timestamp)
  │     ├── Validate against pvdaq-v1.json schema
  │     ├── Idempotency check (site_id + filename + measdatetime)
  │     ├── Build CloudEvents envelope
  │     └── Emit to Service Bus topic
  ├── Mark file as completed in tracking table
  └── Emit invocation metrics
```

## Running Tests

```bash
# All tests
pytest

# Only feature 002 tests
pytest tests/unit/test_oedi_historical_client.py tests/unit/test_csv_normalizer.py tests/integration/test_historical_pipeline.py

# With coverage
pytest --cov=src --cov-report=term-missing
```

## Key Modules

| Module | Purpose |
| --- | --- |
| `src/oedi_historical_client.py` | S3 listing + streaming CSV download |
| `src/csv_normalizer.py` | Timestamp auto-detect, suffix stripping, SiteID injection |
| `src/config.py` | Extended with feature 002 config fields |
| `src/idempotency_store.py` | Extended RowKey format for category-aware keys |
| `src/cloudevents_envelope.py` | Extended to parameterize event type |
| `function_app.py` | `historical_dispatcher` + `historical_worker` functions |

## Shared Modules (from feature 001, unchanged)

| Module | Purpose |
| --- | --- |
| `src/service_bus_emitter.py` | Service Bus topic/queue sender |
| `src/observability.py` | Structured logging, metrics |
| `src/schema_validator.py` | JSON Schema validation |
