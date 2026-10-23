# AGENTS.md — ingestion-func

## 1. Purpose

Pulls PVDAQ solar telemetry from the OEDI S3 data lake and lands it in ADLS Gen2
`bronze`. Inputs: OEDI public S3 CSVs (systems + per-site daily/historical data).
Outputs: bronze blobs in ADLS Gen2, `PvdaqFileTracking`/idempotency rows in Azure
Table Storage, and CloudEvents (`raw.pvdaq.generation.v1`, `solar.pvdaq.dataset.available`)
emitted to Service Bus (`raw-energy-events` queue and dead-letter queue).

## 2. Structure

- `function_app.py` — entry points: `historical_dispatcher` (timer, discovers historical
  files via S3 ListObjectsV2, validates + enqueues one work item per file) and
  `historical_worker` (Service Bus queue trigger: streams S3 → ADLS per file, writes the
  `metadata.json` sidecar, builds + validates the dataset-available CloudEvent, emits it,
  then marks the file completed). The legacy `pvdaq_ingestion` daily-polling function and
  its per-record pipeline were retired in R2.9 — the PVDAQ source holds only historical
  data, and nothing downstream consumed it.
- `src/` — implementation: `oedi_historical_client.py` (S3 listing + streaming),
  `adls_store.py`/`file_tracking_store.py`/`service_bus_emitter.py` (ADLS/table/Service Bus
  stores), `cloudevents_envelope.py` (dataset-available envelope builder, contract-validated),
  `config.py`, `observability.py`.
- `schemas/` — JSON Schema contracts (shared — see §5).
- `tests/unit/`, `tests/contract/`, `tests/integration/` — see §3.
- `specs/` — spec-kit feature specs.

## 3. Commands

Run from this directory (repo root; tests import `src.*`).

| Purpose | Command | Notes |
| :--- | :--- | :--- |
| Setup | `pip install -r requirements-dev.txt` | Works. |
| Lint | `ruff check .` | Works — passes clean. `ruff.toml` excludes `.claude/` and `specs/`. |
| Format check | `ruff format --check .` | Works — passes clean (same excludes). |
| Unit + contract tests | `pytest tests/unit tests/contract` | Works — 281 passed. `tests/contract/` holds `test_cloudevents_envelope_dataset.py` (the dataset-available envelope vs. the vendored contract) — moved here from `tests/unit/` in R2.9c; it was an empty package before that. |
| All tests | `pytest` | Works — 332 passed. Almost all of `tests/integration/` is mocked (respx/`AsyncMock`); one test (`test_adls_store_azurite.py`, marked `azurite`) needs a live Azurite and is skipped unless `AZURITE_TEST_CONNECTION` is set. |
| Run locally | `func start` | **Not run.** Needs Azurite + Service Bus emulator from the workspace root `docker-compose.yml`. |

No separate build step (Python, no compile).

## 4. Conventions

- **One dataset-level pipeline, inline in `function_app.py`:** there is no separate
  per-record pipeline module anymore (`src/record_pipeline.py` and its `process_record`
  were retired in R2.9 along with `pvdaq_ingestion`). `historical_dispatcher` validates
  each work item against `schemas/work-item.v1.json` immediately before
  `send_queue_message`; `historical_worker` runs one guarded sequence per message:
  parse body → validate work item → stream S3→ADLS → write + validate `metadata.json`
  → build + validate the dataset-available CloudEvent → emit → mark completed (last,
  so "completed" means "event published").
- **Idempotency:** file-level only, via `FileTrackingStore` (Azure Table Storage) —
  `mark_queued` before the dispatcher sends a work item, `mark_processing`/`mark_completed`/
  `mark_failed` around the worker's run. There is no per-record idempotency store anymore
  (`src/idempotency_store.py` was feature-001-only and was retired in R2.9).
- **Retries:** outbound HTTP to OEDI goes through `src/http_retry.py::get_with_retry`
  (shared helper, exponential backoff, 3 retries, 404s returned as-is) — do not
  reimplement retry loops in new OEDI clients.
- **Auth:** Azure SDK clients (`ServiceBusClient`, `TableClient`, ADLS) use
  `DefaultAzureCredential` (async) — no connection strings/keys in code.
- **Logging:** structured JSON only, via `src/observability.py::create_logger`
  (`_CorrelatedAdapter`), never bare `print`/`logging.getLogger` in function code —
  every log line carries `correlation_id`, `function_name`, `vendor`. Dataset-level
  metrics are emitted through `emit_dataset_metrics`, not ad hoc log statements. No
  OpenTelemetry/App Insights SDK is wired in yet.
- **Validation/processing failures never raise into the trigger:** every dead-letter send
  routes through `function_app.py::_dead_letter`, a single choke point that tags the
  message with a distinct reason code (`malformed_body`, `validation_failure`,
  `metadata_validation_failure`, `envelope_validation_failure`, `dataset_failure`) instead
  of failing the invocation outright. A transient failure in the dead-letter send itself
  (`DeadLetterSendError`) re-raises so the original message is redelivered rather than
  silently dropped.

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
