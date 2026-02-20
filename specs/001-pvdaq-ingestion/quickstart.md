# Quickstart: PVDAQ Ingestion

**Feature**: 001-pvdaq-ingestion
**Date**: 2026-02-19

## Prerequisites

- Python 3.11+
- Azure Functions Core Tools v4 (`npm install -g azure-functions-core-tools@4`)
- Azure CLI (`az`) with active subscription
- An NREL Developer API key (register at https://developer.nrel.gov/signup/)

## Local Setup

### 1. Create virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # Linux/macOS
.venv\Scripts\activate      # Windows
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure local settings

Copy the template and fill in values:

```bash
cp local.settings.json.template local.settings.json
```

Required settings in `local.settings.json`:

```json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "PVDAQ_API_BASE_URL": "https://developer.nrel.gov/api/pvdaq/v3",
    "PVDAQ_API_KEY": "<your-nrel-api-key>",
    "PVDAQ_SITE_IDS": "2,34",
    "PVDAQ_LOOKBACK_HOURS": "24",
    "PVDAQ_CRON_SCHEDULE": "0 0 */6 * * *",
    "SERVICE_BUS_TOPIC_NAME": "raw-energy-events",
    "DEAD_LETTER_QUEUE_NAME": "pvdaq-dead-letter",
    "ServiceBusConnection__fullyQualifiedNamespace": "<namespace>.servicebus.windows.net",
    "IDEMPOTENCY_TABLE_NAME": "pvdaqidempotency",
    "TableStorageConnection__tableServiceUri": "https://<account>.table.core.windows.net",
    "TENANT_ID": "research",
    "MAPPING_VERSION_PVDAQ": "unknown",
    "SCHEMA_VERSION_PVDAQ": "v1"
  }
}
```

**Note**: For local development, `DefaultAzureCredential` authenticates to Azure
services using your developer identity (`az login`). The `PVDAQ_API_KEY` is the
only secret needed directly in local settings — in deployed environments, this is
resolved via a Key Vault reference.

### 4. Start local Azure services (optional)

For idempotency store testing with Azurite:

```bash
azurite --silent --location .azurite --debug .azurite/debug.log
```

### 5. Run the function locally

```bash
func start
```

The timer trigger fires according to `PVDAQ_CRON_SCHEDULE`. To trigger immediately
for testing, send an admin request:

```bash
curl -X POST http://localhost:7071/admin/functions/pvdaq_ingest \
  -H "Content-Type: application/json" \
  -d '{}'
```

## Running Tests

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# Contract tests only
pytest tests/contract/

# Integration tests only
pytest tests/integration/

# With coverage
pytest --cov=src --cov-report=term-missing
```

## Project Layout

```
function_app.py              # Timer trigger entry point
host.json                    # Function host config
requirements.txt             # Python dependencies
src/
├── pvdaq_access.py          # PVDAQ API client (httpx)
├── schema_validator.py      # JSON Schema validation
├── cloudevents_envelope.py  # CloudEvents construction
├── idempotency_store.py     # Table Storage dedup
├── service_bus_emitter.py   # Service Bus emission
├── observability.py         # Structured logging/metrics
└── config.py                # Configuration loader
schemas/
└── pvdaq-v1.json            # Validation schema
tests/
├── contract/                # Emission contract tests
├── integration/             # Mock API + Service Bus tests
└── unit/                    # Pure logic tests
```

## Key Commands

| Command | Description |
|---------|-------------|
| `func start` | Start function locally |
| `pytest` | Run all tests |
| `pytest tests/contract/` | Run emission contract tests |
| `az login` | Authenticate for local DefaultAzureCredential |
| `func azure functionapp publish <app-name>` | Deploy to Azure |

## Environment Variables Reference

See [data-model.md — Configuration Entities](data-model.md#configuration-entities) for the
complete list of externalized configuration settings.
