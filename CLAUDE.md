# ingestion_func Development Guidelines

Auto-generated from all feature plans. Last updated: 2026-09-23

## Active Technologies
- Python 3.11+, Azure Functions v4 (v2 programming model)
- azure-functions, azure-servicebus, azure-data-tables, azure-identity, azure-storage-file-datalake, httpx, aiohttp, jsonschema
- Azure Table Storage (idempotency store, `PvdaqFileTracking`), ADLS Gen2 bronze container (`raw`), Azure Service Bus (emission + work item queue)

## Project Structure

```text
src/                 # function code (imported as `src.*`)
tests/unit/          # unit tests
tests/contract/      # schema / message contract tests
tests/integration/   # tests against Azurite + Service Bus emulator
schemas/             # JSON schemas (shared contract — see workspace AGENTS.md)
specs/               # spec-kit feature specs
function_app.py      # function entry points
```

## Commands

Run from the repo root (tests import `src.*`):

```bash
pip install -r requirements-dev.txt
ruff check .
pytest
func start          # needs Azurite + Service Bus emulator from the workspace docker-compose.yml
```

## Code Style

`ruff check .` must pass. Do not hand-fix style issues ruff can report.

## Recent Changes
- 002-pvdaq-historical-ingestion: historical ingestion into ADLS Gen2 bronze, file tracking table, aiohttp
- 001-pvdaq-ingestion: PVDAQ ingestion, schema validation, idempotency store, Service Bus emission

<!-- MANUAL ADDITIONS START -->
## Rules

- Only edit files inside this repo. Changes to `schemas/` or message shapes go through the Contract Owner (see workspace `docs/agent-fleet.md`).
- Run `ruff check .` and `pytest` before handing off.
- Skills in `.claude/skills/` load on demand. Use `python-design-patterns` for design changes and `pytest-coverage` for test work; do not read them all up front.
- Never commit `local.settings.json`, `.env` or Azurite data; use `local.settings.json.template`.
<!-- MANUAL ADDITIONS END -->
