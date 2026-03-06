# ingestion_func Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-02-19

## Active Technologies
- Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model) + azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema (all already in requirements.txt) (002-pvdaq-historical-ingestion)
- Azure Table Storage (idempotency store — reused from 001), Azure Service Bus (emission + work item queue) (002-pvdaq-historical-ingestion)

- Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model) + azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema (001-pvdaq-ingestion)

## Project Structure

```text
src/
tests/
```

## Commands

cd src; pytest; ruff check .

## Code Style

Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model): Follow standard conventions

## Recent Changes
- 002-pvdaq-historical-ingestion: Added Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model) + azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema (all already in requirements.txt)

- 001-pvdaq-ingestion: Added Python 3.11+ (Azure Functions v4 Isolated Worker, v2 programming model) + azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema

<!-- MANUAL ADDITIONS START -->
## Skills & Standards

Read all skill files in `.claude/skills/` before making any changes.
Apply these standards to all code in this project.


<!-- MANUAL ADDITIONS END -->
