@AGENTS.md

# ingestion_func Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-09-23

## Active Technologies
- Python 3.11+, Azure Functions v4 (v2 programming model)
- azure-functions, azure-servicebus, azure-data-tables, azure-identity, azure-storage-file-datalake, httpx, aiohttp, jsonschema
- Azure Table Storage (idempotency store, `PvdaqFileTracking`), ADLS Gen2 bronze container (`raw`), Azure Service Bus (emission + work item queue)

## Recent Changes
- 002-pvdaq-historical-ingestion: historical ingestion into ADLS Gen2 bronze, file tracking table, aiohttp
- 001-pvdaq-ingestion: PVDAQ ingestion, schema validation, idempotency store, Service Bus emission

<!-- MANUAL ADDITIONS START -->
<!-- MANUAL ADDITIONS END -->
