# Implementation Plan: PVDAQ Ingestion

**Branch**: `001-pvdaq-ingestion` | **Date**: 2026-02-19 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/specs/001-pvdaq-ingestion/spec.md`

## Summary

Boundary-only ingestion of NREL PVDAQ photovoltaic telemetry data. An Azure Functions
timer-triggered Python function polls the PVDAQ API on a configurable schedule, validates
each record against a versioned JSON schema, enriches it with a CloudEvents-compatible
envelope, checks idempotency via Azure Table Storage, and emits raw events to a Service
Bus topic. Invalid records are dead-lettered; all invocations produce structured
observability telemetry.

## Technical Context

**Language/Version**: Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model)
**Primary Dependencies**: azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema
**Storage**: Azure Table Storage (idempotency store)
**Testing**: pytest, pytest-asyncio, respx (httpx mocking)
**Target Platform**: Azure Functions v4 (Python Isolated Worker) on Linux Consumption/Premium plan
**Project Type**: single
**Performance Goals**: Process up to 10,000 records per invocation within 10-minute function timeout
**Constraints**: Sequential site polling (respect NREL rate limits ~1,000 req/hr); function timeout configurable via host.json; no PII in logs
**Scale/Scope**: Phase 1: 1–10 PVDAQ sites, ~1,000 records/site/poll, single timer function

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| # | Principle | Status | Evidence |
|---|-----------|--------|----------|
| I | Function Isolation | PASS | Single function `pvdaq_ingest` with its own timer trigger; no shared in-process state; errors handled per-site with partial-failure resilience (FR-012) |
| II | Schema Validation at Boundary | PASS | FR-004 validates every record against `pvdaq-v1.json` as first step after deserialization; schema stored in `schemas/` directory; naming follows `{vendor}-v{major}.json` convention |
| III | Metadata Enrichment | PASS | FR-006 defines CloudEvents envelope with all required attributes: `source_vendor`, `ingestion_timestamp`, `schema_version`, `mapping_version` (from env var with `"unknown"` sentinel), `correlation_id` |
| IV | Managed Identity | PASS | FR-010 requires secrets via Key Vault + Managed Identity; Service Bus via identity-based `fullyQualifiedNamespace` connection (R3, R6); local dev uses `DefaultAzureCredential` |
| V | Structured Observability | PASS | FR-009 defines structured telemetry per invocation; all logs structured JSON with `correlation_id`, `function_name`, `vendor` |
| VI | Idempotency | PASS | FR-008 uses composite key (site ID + timestamp); R5 specifies write-before-emit pattern with Table Storage `create_entity()` conditional insert; 24-hour TTL minimum |
| VII | Event Emission Rules | PASS | FR-006/FR-007 define CloudEvents envelope; event type `raw.pvdaq.generation.v1` follows naming convention; topic `raw-energy-events` is configuration-driven; dead-letter on retry exhaustion (FR-005) |

**Post-design re-check**: All 7 principles satisfied. No violations requiring justification.

## Project Structure

### Documentation (this feature)

```text
specs/001-pvdaq-ingestion/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
│   ├── cloudevents-envelope.json    # CloudEvents JSON Schema
│   ├── dead-letter-message.json     # Dead-letter message schema
│   └── pvdaq-v1.json                # PVDAQ record validation schema
├── checklists/
│   └── requirements.md
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
function_app.py              # Azure Functions v2 entry point (timer trigger)
host.json                    # Function host configuration
local.settings.json          # Local dev settings (gitignored)
requirements.txt             # Python dependencies

src/
├── __init__.py
├── pvdaq_access.py          # Thin NREL PVDAQ API client (httpx)
├── schema_validator.py      # JSON Schema validation logic
├── cloudevents_envelope.py  # CloudEvents envelope construction
├── idempotency_store.py     # Azure Table Storage idempotency
├── service_bus_emitter.py   # Service Bus SDK emission (topic + DLQ)
├── observability.py         # Structured logging and metrics
└── config.py                # Externalized configuration loader

schemas/
├── pvdaq-v1.json            # PVDAQ telemetry validation schema
└── README.md                # Schema discovery and naming convention

tests/
├── conftest.py              # Shared fixtures
├── contract/
│   ├── test_cloudevents_envelope.py  # Emission contract tests
│   ├── test_valid_payload.py         # Valid PVDAQ payload sample
│   └── test_invalid_payload.py       # Malformed payload sample
├── integration/
│   ├── test_pvdaq_api.py             # Mock PVDAQ API response
│   ├── test_service_bus_emission.py  # Verify emission to topic
│   └── test_dead_letter.py           # Verify DLQ routing
└── unit/
    ├── test_schema_validation.py     # Schema pass/fail cases
    ├── test_idempotency.py           # Dedup logic
    ├── test_metadata_enrichment.py   # CloudEvents envelope correctness
    ├── test_pvdaq_access.py          # API client retry/error handling
    └── test_config.py                # Configuration loading
```

**Structure Decision**: Single-project layout. Azure Functions v2 model uses a top-level
`function_app.py` as the entry point with decorator-based trigger registration. Domain
logic resides in `src/` to keep the function entry point thin. Schemas live at repository
root in `schemas/` per constitution Principle II. Tests follow contract/integration/unit
hierarchy.

## Complexity Tracking

> No constitution violations detected. Table intentionally left empty.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| — | — | — |
