# Implementation Plan: PVDAQ Historical Data Ingestion

**Branch**: `002-pvdaq-historical-ingestion` | **Date**: 2026-03-02 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/002-pvdaq-historical-ingestion/spec.md`

## Summary

Download historical photovoltaic telemetry CSV files from the OEDI Data Lake S3 bucket for 4 specific PVDAQ sites (9068, 9069, 2107, 7333). Uses a fan-out architecture: a timer-triggered dispatcher function lists CSV files and enqueues work items to a Service Bus queue; a queue-triggered worker function downloads and processes one CSV file per invocation. Each valid record is emitted as a CloudEvents v1.0 envelope. Reuses shared modules from feature 001 (Service Bus emitter, idempotency store, observability, CloudEvents envelope builder).

## Technical Context

**Language/Version**: Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model)
**Primary Dependencies**: azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema (all already in requirements.txt)
**Storage**: Azure Table Storage (idempotency store — reused from 001), Azure Service Bus (emission + work item queue)
**Testing**: pytest, pytest-asyncio, respx (HTTP mocking), unittest.mock
**Target Platform**: Azure Functions (Consumption or Premium plan)
**Project Type**: Single function app with multiple functions
**Performance Goals**: Process large CSV files (up to 870 MB) within per-function timeout; bounded memory via streaming reads
**Constraints**: Azure Function timeout (10 min Consumption / 30 min Premium); per-message Service Bus size limit (256 KB standard / 100 MB premium); S3 public bucket rate limits
**Scale/Scope**: 4 sites, ~10-40 CSV files total, files ranging 7 MB to 870 MB, 5-minute measurement intervals spanning years of historical data

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
| --- | --- | --- |
| I. Function Isolation | PASS | Dispatcher and worker are separate functions with independent triggers. Feature 002 functions are independent from feature 001 function. |
| II. Schema Validation at Boundary | PASS | Records validated against JSON Schema before emission. Schema versioned in `schemas/`. Reuse existing `pvdaq-v1.json` since `additionalProperties: true` accommodates varying columns. |
| III. Metadata Enrichment | PASS | CloudEvents envelope built via shared `build_envelope()`. Includes source_vendor, schema_version, mapping_version, correlation_id, ingestion_timestamp. Event type parameterized for historical data. |
| IV. Managed Identity | PASS | Service Bus and Table Storage accessed via `DefaultAzureCredential`. OEDI S3 is public HTTPS (no auth needed). |
| V. Structured Observability | PASS | Reuses shared `create_logger()`, `InvocationStats`, `emit_invocation_metrics()`. JSON-structured logs with correlation_id. |
| VI. Idempotency | PASS | Write-before-emit pattern via shared `IdempotencyStore`. Key extended to `site_id + category + timestamp` for feature 002 to avoid cross-category collision. |
| VII. Event Emission Rules | PASS | New event type `raw.pvdaq.historical.v1` registered in `topics.md`. CloudEvents envelope with required extension attributes. Dead-letter on validation failure. |

**Post-Phase-1 Re-check**: All principles remain PASS. The fan-out pattern (dispatcher + queue + worker) strengthens isolation (Principle I) and enables per-file timeout management.

## Project Structure

### Documentation (this feature)

```text
specs/002-pvdaq-historical-ingestion/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── work-item-message.json
│   └── file-tracking-entity.json
└── tasks.md             # Phase 2 output (via /speckit.tasks)
```

### Source Code (repository root)

```text
src/
├── __init__.py                    # (existing)
├── cloudevents_envelope.py        # (existing — extend to parameterize event type)
├── config.py                      # (existing — extend with 002 config fields)
├── idempotency_store.py           # (existing — extend _row_key for category-aware keys)
├── observability.py               # (existing — reuse as-is)
├── oedi_data_lake.py              # (existing — feature 001 client, unchanged)
├── oedi_historical_client.py      # NEW — S3 listing + streaming CSV download for 2023-solar-data-prize
├── csv_normalizer.py              # NEW — timestamp auto-detect, sensor suffix stripping, numeric casting
├── schema_validator.py            # (existing — reuse as-is, pvdaq-v1.json already permissive)
└── service_bus_emitter.py         # (existing — reuse as-is)

function_app.py                    # (existing — add historical_dispatcher + historical_worker functions)

schemas/
└── pvdaq-v1.json                  # (existing — reuse, additionalProperties: true)

tests/
├── unit/
│   ├── test_oedi_historical_client.py  # NEW — S3 listing, streaming download, retry
│   ├── test_csv_normalizer.py          # NEW — timestamp detection, suffix strip, normalization
│   └── ...                             # (existing unit tests unchanged)
├── integration/
│   ├── test_historical_pipeline.py     # NEW — dispatcher + worker end-to-end flow
│   └── ...                             # (existing integration tests unchanged)
└── contract/
    └── test_historical_envelope.py     # NEW — CloudEvents contract for historical event type
```

**Structure Decision**: Single function app with feature 001 and feature 002 functions coexisting. Shared modules in `src/` are extended (not replaced) to support both features. New modules added for 002-specific logic (S3 listing, streaming download, CSV normalization with timestamp auto-detect).

## Complexity Tracking

No constitution violations requiring justification. All principles pass.
