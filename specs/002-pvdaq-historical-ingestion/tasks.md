# Tasks: PVDAQ Historical Dataset Ingestion

**Input**: Design documents from `/specs/002-pvdaq-historical-ingestion/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Add new dependency, config fields, and register the new event type

- [x] T001 Add `azure-storage-file-datalake>=12.14.0,<13.0.0` to requirements.txt
- [x] T002 Add ADLS config fields (`adls_account_url`, `adls_container_name`) to `HistoricalConfig` in src/config.py and validate in `load_historical_config()`
- [x] T003 [P] Add `ADLS_ACCOUNT_URL` and `ADLS_CONTAINER_NAME` to local.settings.json
- [x] T004 [P] Register event type `solar.pvdaq.dataset.available` in topics.md per Constitution VII
- [x] T004b [P] Configure queue trigger scaling in host.json — set `maxConcurrentCalls: 1` and `batchOptions.maxMessageCount: 1` for pvdaq-historical-work queue per Constitution Development Workflow

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core modules that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Create ADLS Gen2 streaming store module in src/adls_store.py — implement `AdlsStore` class with `stream_upload(source_url, file_path) -> (bytes_written, sha256_hex)` using create_file → append_data → flush_data pattern with 4 MiB chunks and incremental SHA-256 hash. Use `DefaultAzureCredential` and async context manager pattern consistent with existing stores.
- [x] T006 [P] Add `build_dataset_envelope()` function to src/cloudevents_envelope.py — builds a CloudEvents v1.0 envelope with type `solar.pvdaq.dataset.available`, data block per contracts/dataset-event.json. Set `mapping_version` to `"unknown"` per Constitution III fallback.
- [x] T007 [P] Add `extract_category(file_name: str, site_id: int) -> str` function to src/oedi_historical_client.py — parse category from filename pattern `{site_id}_{category}_data.csv` with fallback to full basename.
- [x] T008 [P] Add `DatasetIngestionStats` dataclass and `emit_dataset_metrics()` to src/observability.py — fields: datasets_discovered, datasets_downloaded, datasets_stored, datasets_emitted, datasets_failed, source, correlation_id, duration_ms.

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 — Bulk Historical CSV Download and Storage (Priority: P1) 🎯 MVP

**Goal**: Download CSV files from OEDI S3 and stream them to ADLS Gen2 at `raw/pvdaq/site_id={id}/year={y}/month={m}/{file}`.

**Independent Test**: Trigger dispatcher, verify CSV files are written to ADLS at the correct partition path with bounded memory usage.

### Implementation for User Story 1

- [x] T009 [US1] Extend `FileTrackingStore.mark_completed()` in src/file_tracking_store.py — add parameters: storage_path, file_hash, ingestion_id, source_url, ingestion_time. Include these fields in the upserted entity.
- [x] T010 [US1] Rewrite `historical_worker()` in function_app.py — replace row-level processing with dataset pipeline: deserialize work item → mark_processing → build S3 source URL → stream_upload to ADLS → update tracking entity → emit event. Remove imports for csv_normalizer, record_pipeline, IdempotencyStore.
- [x] T011 [US1] Update `historical_dispatcher()` in function_app.py — add category to work item message; preserve mark_queued → send_queue_message ordering per Constitution VI.
- [x] T012 [US1] Fix ADLS destination path in historical_worker — **CORRECTION REQUIRED** (Constitution VIII gate failure): current path `pvdaq/site_id={site_id}/category={category}/{file_name}` is wrong. Replace with `raw/pvdaq/site_id={site_id}/year={year}/month={month:02d}/{file_name}` where `year`/`month` are derived in UTC from `last_modified` (falls back to ingestion date). Extract helper `_adls_path(site_id: int, last_modified: str, file_name: str) -> str` at module level in function_app.py.
- [x] T013 [US1] Add `last_modified` to work-item dict in historical_dispatcher — in function_app.py dispatcher loop, add `"last_modified": file_info.get("last_modified", "")` to the work_item dict before calling `send_queue_message`. This enables the worker to derive year/month for ADLS path without an extra S3 call.
- [x] T014 [US1] Fix integration test path assertion in tests/integration/test_historical_pipeline.py — update `test_worker_deterministic_adls_path` to assert `raw/pvdaq/site_id=9068/year=2024/month=01/9068_ac_power_data.csv` (derived from fixture `last_modified: "2024-01-15T12:00:00Z"`). Add `last_modified` field to `_default_work_item()` helper. Update `_mock_work_item_msg` usages accordingly.

**Checkpoint**: User Story 1 complete — files stream from S3 to ADLS with correct `raw/pvdaq/year/month` partitioning

---

## Phase 4: User Story 2 — Dataset Metadata Registration (Priority: P1)

**Goal**: Register metadata (storage_path, file_hash, ingestion_id, source_url, file_size, ingestion_time) in the file tracking table on completion.

**Independent Test**: Ingest a file, query `PvdaqFileTracking` table, verify entity contains all metadata fields at the correct values.

### Implementation for User Story 2

- [x] T015 [US2] Wire metadata fields in historical_worker — after stream_upload, call `mark_completed()` with storage_path, file_hash (from stream_upload return), ingestion_id (uuid), source_url, ingestion_time (datetime.now UTC). Ensure ingestion_id is generated once per work item and reused in both metadata and event.
- [x] T016 [US2] Validate contracts/file-tracking-entity.json — confirm schema matches implementation fields (StoragePath, FileHash, IngestionId, SourceUrl, IngestionTime, Status lifecycle).
- [x] T017 [US2] Add work-item schema validation in historical_worker in function_app.py — at top of `historical_worker`, before `mark_processing`, validate the parsed work_item dict against `specs/002-pvdaq-historical-ingestion/contracts/work-item-message.json` using `src/schema_validator.py`. On validation failure: call `emit_dead_letter` with `error_type: "validation_failure"` and `file_reference: <raw message body>`, then return without raising (prevents automatic retry of malformed messages). This fulfills Constitution II for queue message ingestion.

**Checkpoint**: User Story 2 complete — metadata is persisted alongside file tracking status

---

## Phase 5: User Story 3 — Dataset Event Emission (Priority: P2)

**Goal**: Emit a `solar.pvdaq.dataset.available` CloudEvent per successfully stored dataset.

**Independent Test**: Ingest a file, verify a CloudEvent with correct type, source, and data block (site_id, category, file_format, storage_path, ingestion_id, source_url, file_size, file_hash) appears on the configured topic.

### Implementation for User Story 3

- [x] T018 [US3] Wire dataset event emission in historical_worker — after metadata registration, build dataset envelope via `build_dataset_envelope()` and emit via `emit_cloudevent()`. Verify all required data block fields are populated.
- [x] T019 [US3] Add dead-letter handling for dataset failures in historical_worker — on download failure, ADLS write failure, or event emission failure: call `mark_failed`, send to dead-letter queue with file_reference, failure_reason, correlation_id via `emit_dead_letter()`, then re-raise.

**Checkpoint**: User Story 3 complete — downstream systems receive dataset notifications

---

## Phase 6: User Story 4 — Incremental File Detection (Priority: P2)

**Goal**: On subsequent runs, skip already-ingested files; re-queue files whose S3 `LastModified` timestamp has changed (per clarification 2026-03-19: `LastModified` comparison only).

**Independent Test**: Run dispatcher twice. Second run should discover same files but enqueue zero. Set a different `last_modified` in the S3 mock for one file, re-run, verify only that file is re-queued.

### Implementation for User Story 4

- [x] T020 [US4] Verify incremental detection in dispatcher — confirm `FileTrackingStore.get_unprocessed_files()` correctly filters out completed files (Status == "completed" AND LastModified unchanged). No code change expected — the existing S3 LastModified comparison is correct.
- [x] T021 [US4] Handle re-ingestion in worker — when a file is re-ingested (status was previously completed), `stream_upload` overwrites the ADLS file and `mark_completed` updates the existing entity with new file_hash and ingestion_time via `upsert_entity`. Verify this works correctly.

**Checkpoint**: User Story 4 complete — incremental runs are efficient

---

## Phase 7: User Story 5 — Observability and Metrics (Priority: P3)

**Goal**: Structured logs with correlation_id and dataset-level summary metrics (datasets_discovered, downloaded, stored, emitted, failed, duration) after each invocation.

**Independent Test**: Trigger ingestion, verify log output includes correlation_id, all FR-011 metric fields, and duration_ms.

### Implementation for User Story 5

- [x] T022 [US5] Integrate `DatasetIngestionStats` in historical_dispatcher in function_app.py — track datasets_discovered per site. Emit metrics via `emit_dataset_metrics()` at end of dispatcher run.
- [x] T023 [US5] Integrate `DatasetIngestionStats` in historical_worker in function_app.py — track datasets_downloaded=1, datasets_stored, datasets_emitted, datasets_failed. Emit metrics at end of worker run.
- [x] T024 [US5] Structured log messages in both dispatcher and worker — replace row-level metric references with dataset-level metrics (discovered, downloaded, stored, emitted, failed).

**Checkpoint**: User Story 5 complete — operations team has dataset-level visibility

---

## Phase 8: Local Emulation (Cross-Cutting — Constitution Local Emulation Contract)

**Goal**: Full local development flow using Azurite for ADLS, Queue, and Table — no real Azure resources needed.

**Independent Test**: Start Azurite, set `STORAGE_EMULATOR=true`, run `func start`, trigger dispatcher manually — verify files land in Azurite Blob container `bronze` and CloudEvent messages appear in Azurite Queue.

### Implementation for Local Emulation

- [x] T025 [P] Create src/azurite_queue_emitter.py — implement `AzuriteQueueEmitter` class wrapping `azure-storage-queue` SDK. Expose the same interface as `ServiceBusEmitter`: `async emit_cloudevent(queue_name, envelope)`, `async emit_dead_letter(queue_name, message_body, ...)`, `async send_queue_message(queue_name, message_body, ...)`. Accept `connection_string` constructor arg (Azurite devstoreaccount1 connection string). Implement async context manager.
- [x] T026 Add emitter factory `_make_emitter(config: HistoricalConfig) -> ServiceBusEmitter | AzuriteQueueEmitter` in function_app.py — returns `AzuriteQueueEmitter(os.environ["AzureWebJobsStorage"])` when `os.environ.get("STORAGE_EMULATOR", "").lower() == "true"`, else `ServiceBusEmitter(config.service_bus_fully_qualified_namespace)`. Update `historical_dispatcher` and `historical_worker` to use `_make_emitter(config)` instead of constructing `ServiceBusEmitter` directly.
- [x] T027 [P] Update local.settings.json — set `"ADLS_ACCOUNT_URL": "http://127.0.0.1:10000/devstoreaccount1"` and `"STORAGE_EMULATOR": "true"` for local development. These override the current production ADLS URL when running locally.

**Checkpoint**: Local development works end-to-end with Azurite only

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup, documentation, and final validation

- [x] T028 [P] Update specs/002-pvdaq-historical-ingestion/quickstart.md — verify architecture diagram reflects corrected ADLS path, Azurite full-service setup, and `STORAGE_EMULATOR=true` flag (partially done in plan revision).
- [x] T029 [P] Verify schemas/README.md — confirm dataset-event.json is listed and `solar.pvdaq.dataset.available` is documented.
- [x] T030 Run full test suite (`cd src && pytest`) — ensure all feature 001 and feature 002 tests pass with corrected ADLS path. Fix any broken assertions.
- [x] T031 Run linting (`cd src && ruff check .`) — fix any new lint warnings from new/modified files.

---

## Phase 10: Bronze Layer Convention Alignment ⚠️ REOPENED

**Purpose**: Align implementation with bronze layer folder structure, versioning, and metadata requirements.
**Trigger**: Compliance review 2026-03-19 — 4 critical gaps identified against bronze layer spec.

**⚠️ CRITICAL**: T010 (historical_worker rewrite) and T012 (ADLS path) are partially invalidated — the path format, file naming, and append-only behaviour must be corrected.

### Versioning Support

- [x] T032 Add `get_versions(site_id, s3_key) -> list[int]` method to `src/file_tracking_store.py` — query `PvdaqFileTracking` for all entities with matching PartitionKey and a filter on `S3Key`, return their `Version` integer values. Returns empty list if no entries exist.
- [x] T033 Update `mark_queued()` and `mark_completed()` in `src/file_tracking_store.py` — add `version: int` and `category: str` parameters. Use `SHA256(s3_key)_v{version}` as RowKey (replacing the current `SHA256(s3_key)`). Add `Version` and `Category` fields to the upserted entity. `mark_completed()` also stores `RowCount` and `MetadataPath`.

### ADLS Path and File Naming

- [x] T034 Rewrite `_adls_path()` helper in `function_app.py` — replace current `raw/pvdaq/site_id=.../year=.../month=.../{file}` with `source=pvdaq/dataset={site_id}_{category}/ingestion_date={YYYY-MM-DD}/{site_id}_{category}_v{version}.csv`. `ingestion_date` is UTC date of ingestion (`datetime.now(timezone.utc).strftime("%Y-%m-%d")`), NOT derived from `last_modified`. Add `_metadata_path(site_id, category, ingestion_date)` helper returning same prefix + `/metadata.json`. Both helpers take `(site_id: int, category: str, ingestion_date: str, version: int)`.
- [x] T035 Update `historical_worker()` in `function_app.py` — before `mark_processing`, call `_next_version = max(await tracker.get_versions(site_id, s3_key), default=0) + 1`. Compute `ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")`. Use `_adls_path(site_id, category, ingestion_date, version)` for the CSV path. After stream upload, write `metadata.json` via `adls.write_json(_metadata_path(...), metadata_dict)` where `metadata_dict` matches `contracts/metadata-file.json`. Pass `version` and `row_count` (returned from updated `stream_upload`) to `mark_completed()`.

### Metadata File Writing

- [x] T036 Add `write_json(file_path: str, data: dict) -> None` method to `src/adls_store.py` — serialise `data` to JSON bytes and write to ADLS using the same `create_file → append_data → flush_data` pattern (single chunk, no streaming needed). Raises `AdlsUploadError` on failure.
- [x] T037 Update `stream_upload()` in `src/adls_store.py` — count `\n` bytes during chunk iteration and return `(bytes_written, sha256_hex, newline_count)` as a 3-tuple. Update all call sites in `function_app.py` to unpack the third element as `row_count`.

### Integration Test Updates

- [x] T038 Update `tests/integration/test_historical_pipeline.py` — update `test_worker_deterministic_adls_path` to assert `source=pvdaq/dataset=9068_ac_power/ingestion_date={today}/9068_ac_power_v1.csv`. Mock `tracker.get_versions` to return `[]` (first ingestion). Add `test_worker_writes_metadata_json` to verify `adls.write_json` is called with correct `metadata-file.json`-conformant payload. Update `_historical_config()`, `_default_work_item()` as needed.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on T001 — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on T005, T007 — T012/T013/T014 are corrections to existing code
- **US2 (Phase 4)**: Depends on US1 completion (T012 path fix) — metadata uses the corrected path
- **US3 (Phase 5)**: Depends on US1+US2 completion — event emission comes after store + metadata
- **US4 (Phase 6)**: Depends on US1 — validates incremental behavior of revised worker
- **US5 (Phase 7)**: Depends on US1 — metrics integrate into dispatcher + worker
- **Local Emulation (Phase 8)**: Depends on US3 completion — needs full pipeline before testing end-to-end locally
- **Polish (Phase 9)**: Depends on all phases complete

### User Story Dependencies

- **US1 (P1)**: T012/T013/T014 are corrections — blocking for US2/US3
- **US2 (P1)**: T017 (schema validation) is independent of other US2 tasks
- **US3 (P2)**: Depends on US1+US2 (corrected path + metadata)
- **US4 (P2)**: Can verify after US1 correction — incremental detection independent
- **US5 (P3)**: Independent of path correction — metrics already integrated

### Remaining Open Tasks

| Task | Phase | Priority | Blocks |
| --- | --- | --- | --- |
| T012 — Fix ADLS path | US1 | P0 (gate) | T013, T014, US2, US3 |
| T013 — Add `last_modified` to work item | US1 | P0 (gate) | T012 |
| T014 — Fix integration test assertion | US1 | P0 (gate) | T030 |
| T017 — Work-item schema validation | US2 | P1 | — |
| T025 — AzuriteQueueEmitter | Local emulation | P1 | T026 |
| T026 — Emitter factory | Local emulation | P1 | T025 |
| T027 — local.settings.json Azurite URL | Local emulation | P1 | — |
| T028–T031 — Polish | Polish | P2 | all above |

### Parallel Opportunities

- T012 + T025 can run in parallel (different files)
- T013 depends on T012 being started (same function, same code block)
- T014 + T027 can run in parallel (different files)
- T025 + T027 can run in parallel (different files)
- T028 + T029 can run in parallel (different docs)

---

## Parallel Example: Correction Phase (Highest Priority)

```text
# P0 gate tasks — run sequentially (T013 depends on T012):
T012: Fix adls_path in function_app.py (historical_worker) → _adls_path() helper
T013: Add last_modified to work-item dict in historical_dispatcher (function_app.py)
T014: Fix test_worker_deterministic_adls_path in tests/integration/test_historical_pipeline.py

# Run in parallel alongside P0 fixes:
T025: Create src/azurite_queue_emitter.py
T027: Update local.settings.json
```

---

## Implementation Strategy

### Immediate Priority (P0 — Gate Failure Resolution)

1. Fix T012 — correct ADLS path in `historical_worker`
2. Fix T013 — add `last_modified` to dispatcher work item
3. Fix T014 — update integration test assertion
4. **Validate**: Run `pytest tests/integration/test_historical_pipeline.py::TestWorkerIntegration::test_worker_deterministic_adls_path` — must pass

### Secondary Priority (P1 — Hardening)

1. T017 — work-item schema validation (Constitution II)
2. T025 + T026 + T027 — local emulation (Constitution Local Emulation Contract)

### Final (P2 — Polish)

1. T028–T031 — docs + linting + full test pass

---

## Notes

- `[x]` = completed in previous implementation sessions
- `[ ]` = open — requires implementation
- T012 was previously marked `[x]` with wrong path; re-opened as a gate failure (Constitution VIII)
- Feature 001 code (csv_normalizer, record_pipeline, schema_validator, idempotency_store) is NOT modified — it remains for feature 001's per-row pipeline
- `schema_validator.py` IS used by feature 002 for work-item validation (T017) — contrary to the original quickstart.md which listed it as unused
- Total tasks: 31 (5 setup + 4 foundational + 6 US1 + 3 US2 + 2 US3 + 2 US4 + 3 US5 + 3 local emulation + 4 polish)
- Open tasks: 10 (T012–T014, T017, T025–T031)
