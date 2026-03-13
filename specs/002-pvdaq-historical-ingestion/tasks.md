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
- [x] T003 [P] Add `ADLS_ACCOUNT_URL` and `ADLS_CONTAINER_NAME` to local.settings.json.template
- [x] T004 [P] Register event type `solar.pvdaq.dataset.available` in topics.md per Constitution VII (domain-level lifecycle convention `{domain}.{vendor}.{entity}.{action}`, Constitution v1.2.0)
- [x] T004b [P] Configure queue trigger scaling in host.json — set `maxConcurrentSessions: 1` and `maxBatchSize: 1` for `pvdaq-historical-work` queue to ensure sequential large-file processing per Constitution Development Workflow

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core modules that MUST be complete before ANY user story can be implemented

**CRITICAL**: No user story work can begin until this phase is complete

- [x] T005 Create ADLS Gen2 streaming store module in src/adls_store.py — implement `AdlsStore` class with `stream_upload(source_url, file_path) -> (bytes_written, sha256_hex)` using create_file → append_data → flush_data pattern with 4 MiB chunks and incremental SHA-256 hash. Use `DefaultAzureCredential` and async context manager pattern consistent with existing stores.
- [x] T006 [P] Create dataset event envelope builder — add `build_dataset_envelope()` function to src/cloudevents_envelope.py that builds a CloudEvents v1.0 envelope with type `solar.pvdaq.dataset.available`, data block per contracts/dataset-event.json. Set `mapping_version` to `"unknown"` per Constitution III fallback (no field mapping at dataset level).
- [x] T007 [P] Add category extraction helper — add `extract_category(file_name: str, site_id: int) -> str` function to src/oedi_historical_client.py that parses category from filename pattern `{site_id}_{category}_data.csv` with fallback to full basename.
- [x] T008 [P] Add dataset-level metrics class — add `DatasetIngestionStats` dataclass to src/observability.py with fields: datasets_discovered, datasets_downloaded, datasets_stored, datasets_emitted, datasets_failed, source, correlation_id, duration_ms. Add `emit_dataset_metrics()` function.

**Checkpoint**: Foundation ready — user story implementation can now begin

---

## Phase 3: User Story 1 — Bulk Historical CSV Download and Storage (Priority: P1) MVP

**Goal**: Download CSV files from OEDI S3 and stream them to ADLS Gen2 raw container at deterministic paths.

**Independent Test**: Trigger dispatcher, verify CSV files are written to ADLS at `/raw/pvdaq/site_id={id}/category={cat}/{file}.csv`.

### Implementation for User Story 1

- [x] T009 [US1] Extend `FileTrackingStore.mark_completed()` in src/file_tracking_store.py — add parameters: storage_path, file_hash, ingestion_id, source_url, ingestion_time. Include these fields in the upserted entity. Remove `records_emitted` parameter (no longer applicable).
- [x] T010 [US1] Rewrite `historical_worker()` in function_app.py — replace row-level processing with dataset pipeline: deserialize work item → mark_processing → build S3 source URL → stream_upload to ADLS via AdlsStore → update tracking entity with metadata → mark completed. Remove imports for csv_normalizer, record_pipeline, IdempotencyStore, schema_validator.
- [x] T011 [US1] Update `historical_dispatcher()` in function_app.py — remove IdempotencyStore context manager (no longer needed). Add category to work item message. Ensure mark_queued → send_queue_message ordering preserved.
- [x] T012 [US1] Build ADLS destination path in historical_worker — implement deterministic path: `/raw/pvdaq/site_id={site_id}/category={category}/{file_name}.csv` using category extracted from filename via `extract_category()`.

**Checkpoint**: User Story 1 complete — files stream from S3 to ADLS with bounded memory

---

## Phase 4: User Story 2 — Dataset Metadata Registration (Priority: P1)

**Goal**: Register metadata (storage_path, file_hash, ingestion_id, source_url, file_size) in the file tracking table on completion.

**Independent Test**: Ingest a file, query `PvdaqFileTracking` table, verify entity contains all metadata fields.

### Implementation for User Story 2

- [x] T013 [US2] Wire metadata fields in historical_worker — after stream_upload completes, call `mark_completed()` with storage_path, file_hash (from stream_upload return), ingestion_id (uuid), source_url, ingestion_time (datetime.now UTC). Ensure ingestion_id is generated once per work item and reused in both metadata and event.
- [x] T014 [US2] Update contracts/file-tracking-entity.json — already done in planning phase, validate schema matches implementation fields.

**Checkpoint**: User Story 2 complete — metadata is persisted alongside file tracking status

---

## Phase 5: User Story 3 — Dataset Event Emission (Priority: P2)

**Goal**: Emit a `solar.pvdaq.dataset.available` CloudEvent per successfully stored dataset.

**Independent Test**: Ingest a file, verify a CloudEvent with correct type and data block appears on the Service Bus topic.

### Implementation for User Story 3

- [x] T015 [US3] Wire dataset event emission in historical_worker — after metadata registration, build dataset envelope via `build_dataset_envelope()` and emit via `ServiceBusEmitter.emit_cloudevent()`. Include: site_id, category, file_format="csv", storage_path, ingestion_id, source_url, file_size, file_hash.
- [x] T016 [US3] Add dead-letter handling for dataset failures in historical_worker — on download failure, ADLS write failure, or event emission failure, send to dead-letter queue with file reference, failure reason, and correlation_id via `emit_dead_letter()`.

**Checkpoint**: User Story 3 complete — downstream systems receive dataset notifications

---

## Phase 6: User Story 4 — Incremental File Detection (Priority: P2)

**Goal**: On subsequent runs, skip already-ingested files; re-queue files that have changed (size or last-modified).

**Independent Test**: Run ingestion twice. Second run should discover same files but enqueue zero (all completed). Modify a file's size in S3 mock, re-run, verify only that file is re-queued.

### Implementation for User Story 4

- [x] T017 [US4] Verify incremental detection in dispatcher — confirm `FileTrackingStore.get_unprocessed_files()` correctly filters out completed files and re-queues changed files. Existing implementation should work as-is since it compares Status, Size, and LastModified. No code change expected — validate via integration test.
- [x] T018 [US4] Handle re-ingestion in worker — when a file is re-ingested (status was previously completed), ensure `stream_upload` overwrites the ADLS file and `mark_completed` updates the existing entity with new file_hash and ingestion_time. Verify `upsert_entity` handles this.

**Checkpoint**: User Story 4 complete — incremental runs are efficient

---

## Phase 7: User Story 5 — Observability and Metrics (Priority: P3)

**Goal**: Structured logs with correlation_id and dataset-level summary metrics after each run.

**Independent Test**: Trigger ingestion, verify log output includes correlation_id, dataset counts (discovered/downloaded/stored/emitted/failed), and duration.

### Implementation for User Story 5

- [x] T019 [US5] Integrate `DatasetIngestionStats` in historical_dispatcher — track datasets_discovered per site, total enqueued. Emit metrics via `emit_dataset_metrics()` at end of dispatcher run.
- [x] T020 [US5] Integrate `DatasetIngestionStats` in historical_worker — track datasets_downloaded=1, datasets_stored (0 or 1), datasets_emitted (0 or 1), datasets_failed (0 or 1). Emit metrics at end of worker run.
- [x] T021 [US5] Update structured log messages in both dispatcher and worker — replace row-level metric references (records_retrieved, valid, invalid, emitted, duplicates) with dataset-level metrics (discovered, downloaded, stored, emitted, failed).

**Checkpoint**: User Story 5 complete — operations team has dataset-level visibility

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Cleanup, documentation, and validation

- [x] T022 [P] Update specs/002-pvdaq-historical-ingestion/quickstart.md — verify architecture diagram and module listing match final implementation
- [x] T023 [P] Update schemas/README.md — add dataset-event.json to schema listing and document the `solar.pvdaq.dataset.available` event type
- [x] T024 Remove unused imports from function_app.py — clean up any remaining references to csv_normalizer, record_pipeline, IdempotencyStore, schema_validator from the historical worker/dispatcher functions
- [x] T025 Run full test suite (`cd src; pytest`) — ensure all existing feature 001 tests still pass (no regressions), fix any broken assertions
- [x] T026 Run linting (`cd src; ruff check .`) — fix any new lint warnings

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on T001 (ADLS package) — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on T005, T007 (AdlsStore, category extraction)
- **US2 (Phase 4)**: Depends on T009 (extended mark_completed)
- **US3 (Phase 5)**: Depends on T006 (dataset envelope builder) and US1+US2 completion
- **US4 (Phase 6)**: Depends on US1 (worker must be rewritten first)
- **US5 (Phase 7)**: Depends on T008 (DatasetIngestionStats) and US1 (worker must exist)
- **Polish (Phase 8)**: Depends on all user stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Foundational — no dependencies on other stories
- **US2 (P1)**: Can start after T009 — integrates with US1 worker (same function)
- **US3 (P2)**: Depends on US1+US2 — event emission comes after store + metadata
- **US4 (P2)**: Depends on US1 — validates incremental behavior of revised worker
- **US5 (P3)**: Depends on US1 — metrics integrate into dispatcher + worker

### Within Each User Story

- Models/stores before services/functions
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- T003 + T004 can run in parallel (different files)
- T005, T006, T007, T008 can all run in parallel (different modules)
- T022 + T023 can run in parallel (different docs)
- US4 (T017-T018) and US5 (T019-T021) can run in parallel after US1+US2+US3

---

## Parallel Example: Foundational Phase

```bash
# Launch all foundational tasks together (different files):
T005: Create AdlsStore in src/adls_store.py
T006: Add dataset envelope builder in src/cloudevents_envelope.py
T007: Add category extraction in src/oedi_historical_client.py
T008: Add DatasetIngestionStats in src/observability.py
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001-T004)
2. Complete Phase 2: Foundational (T005-T008)
3. Complete Phase 3: User Story 1 (T009-T012)
4. **STOP and VALIDATE**: Trigger dispatcher + worker, verify files land in ADLS
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. US1 (download + store) → Validate files in ADLS (MVP!)
3. US2 (metadata) → Validate tracking table has metadata fields
4. US3 (event emission) → Validate CloudEvent on topic
5. US4 (incremental) → Validate second run is fast
6. US5 (observability) → Validate structured logs
7. Polish → Clean up, docs, full test pass

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Feature 001 code (csv_normalizer, record_pipeline, schema_validator, idempotency_store) is NOT modified or removed — it remains for feature 001's per-row pipeline
- The worker function is the primary change target — it transforms from row-level processing to dataset-level store+emit
- Total tasks: 26 (4 setup + 4 foundational + 4 US1 + 2 US2 + 2 US3 + 2 US4 + 3 US5 + 5 polish)
