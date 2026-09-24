# AGENTS.md — ingestion-func

## 1. Purpose

Pulls PVDAQ solar telemetry from the OEDI S3 data lake and lands it in ADLS Gen2
`bronze`. Inputs: OEDI public S3 CSVs (systems + per-site daily/historical data).
Outputs: bronze blobs in ADLS Gen2, `PvdaqFileTracking`/idempotency rows in Azure
Table Storage, and CloudEvents (`raw.pvdaq.generation.v1`, `solar.pvdaq.dataset.available`)
emitted to Service Bus (`raw-energy-events` queue and dead-letter queue).

## 2. Structure

- `function_app.py` — entry points: `pvdaq_ingestion` (timer, daily), `historical_dispatcher`
  (timer, discovers historical files), `historical_worker` (Service Bus queue trigger, streams
  S3 → ADLS per file).
- `src/` — implementation: OEDI clients, ADLS/table/Service Bus stores, schema validation,
  the shared `record_pipeline.py` (validate → dead-letter → idempotency → emit), observability.
- `schemas/` — JSON Schema contracts (shared — see §5).
- `tests/unit/`, `tests/contract/`, `tests/integration/` — see §3.
- `specs/` — spec-kit feature specs.

## 3. Commands

Run from this directory (repo root; tests import `src.*`).

| Purpose | Command | Notes |
| :--- | :--- | :--- |
| Setup | `pip install -r requirements-dev.txt` | Works. |
| Lint | `ruff check .` | Works — passes clean. `ruff.toml` excludes `.claude/`. |
| Unit + contract tests | `pytest tests/unit tests/contract` | Works — 173 passed. |
| All tests | `pytest` | Works — 197 passed. `tests/integration/` is fully mocked (respx/`AsyncMock`); no live Azurite/Service Bus needed to pass today. |
| Run locally | `func start` | **Not run.** Needs Azurite + Service Bus emulator from the workspace root `docker-compose.yml`. |

No separate build step (Python, no compile).

## 4. Conventions

- **Shared pipeline, not per-function duplication:** both timer functions funnel
  per-record work through `src/record_pipeline.py::process_record` (validate → dead-letter
  → idempotency check → build CloudEvents envelope → emit → mark completed).
- **Idempotency:** write-before-emit via `IdempotencyStore` (Azure Table Storage conditional
  insert, `IdempotencyResult.NEW/DUPLICATE/RETRY_EMIT`). Historical file processing uses the
  same pattern via `FileTrackingStore` (`mark_queued` before send, `mark_completed` after).
- **Retries:** outbound HTTP to OEDI goes through `src/http_retry.py::get_with_retry`
  (shared helper, exponential backoff, 3 retries, 404s returned as-is) — do not
  reimplement retry loops in new OEDI clients.
- **Auth:** Azure SDK clients (`ServiceBusClient`, `TableClient`, ADLS) use
  `DefaultAzureCredential` (async) — no connection strings/keys in code.
- **Logging:** structured JSON only, via `src/observability.py::create_logger`
  (`_CorrelatedAdapter`), never bare `print`/`logging.getLogger` in function code —
  every log line carries `correlation_id`, `function_name`, `vendor`. Metrics are
  emitted through `emit_dataset_metrics`/`emit_invocation_metrics`/`emit_warning_metric`,
  not ad hoc log statements. No OpenTelemetry/App Insights SDK is wired in yet.
- **Validation failures never raise into the trigger:** invalid records are routed to
  the dead-letter queue (`build_dead_letter_message`) instead of failing the invocation.

## 5. Boundaries

- Edit only files inside this repo (`services/ingestion-func/`).
- `schemas/` and message/CloudEvents envelope shapes are shared contracts — changes go
  through the Contract Owner role (workspace `docs/agent-fleet.md` §3.4), not this repo alone.
- Never commit `local.settings.json`, `.env`, or Azurite runtime data (`azurite-data/`,
  `__azurite_db_*`, `AzuriteConfig`) — use `local.settings.json.template`.

## 6. Skills

- `python-design-patterns` — load for design/architecture changes (layering, abstractions).
- `pytest-coverage` — load for test work (running with coverage, closing gaps).
- `dockerfile` — load when changing this service's Dockerfile or docker-compose entry.
- `az-cost-optimize` — load when asked to review/reduce Azure resource cost here.
- `excalidraw-diagram-generator` — load when asked to produce/update a diagram for this service.
