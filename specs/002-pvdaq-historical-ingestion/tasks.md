# Tasks: PVDAQ Historical Data Ingestion

**Input**: Design documents from `/specs/002-pvdaq-historical-ingestion/`
**Prerequisites**: plan.md (required), spec.md (required), research.md, data-model.md, contracts/

**Tests**: Tests are included as they follow the existing project pattern (feature 001 has 110 tests at 100% coverage).

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization, configuration, and shared module extensions

- [x] T001 Extend `src/config.py` with feature 002 configuration fields: `PVDAQ_HISTORICAL_SITE_IDS`, `PVDAQ_HISTORICAL_CRON_SCHEDULE`, `PVDAQ_HISTORICAL_QUEUE_NAME`, `FILE_TRACKING_TABLE_NAME`, `OEDI_HISTORICAL_PREFIX`. Add a `HistoricalConfig` dataclass and `load_historical_config()` function alongside the existing `Config`/`load_config()`.
- [x] T002 [P] Update `local.settings.json.template` with feature 002 environment variables: historical site IDs (9068,9069,2107,7333), cron schedule, queue name, file tracking table name, OEDI historical prefix.
- [x] T003 [P] Register event type `raw.pvdaq.historical.v1` in `topics.md` with topic `raw-energy-events`, owner `historical_worker`, and schema `pvdaq-v1.json`.
- [x] T004 [P] Update `host.json` to add queue trigger configuration for the historical worker: `batchSize`, `maxBatchSize`, `maxConcurrentCalls`, and `visibilityTimeout` under `extensions.serviceBus` per constitution Principle I (concurrency/scaling limits).
- [x] T005 [P] Add unit tests for the new config fields in `tests/unit/test_config.py`: test `load_historical_config()` with valid env, test missing required fields raise `ConfigurationError`, test site ID parsing.

**Checkpoint**: Configuration and infrastructure ready for feature 002 development.

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core modules that ALL user stories depend on. MUST complete before any user story work begins.

**CRITICAL**: No user story work can begin until this phase is complete.

- [x] T006 Create `src/csv_normalizer.py` with: (a) `detect_timestamp_column(headers: list[str]) -> str | None` that searches for `measured_on`, `timestamp`, `Date-Time`, `datetime` in priority order; (b) `normalize_historical_record(row: dict, site_id: int, file_name: str) -> dict | None` that injects `SiteID` from parameter (not from CSV), maps detected timestamp column to `measdatetime`, strips sensor suffixes using regex `r"(?:_o)?_\d+$"`, and casts numeric values; (c) `SENSOR_SUFFIX_PATTERN = re.compile(r"(?:_o)?_\d+$")`.
- [x] T007 [P] Create `src/oedi_historical_client.py` with class `OediHistoricalClient`: (a) `list_csv_files(site_id: int) -> list[dict]` — calls S3 ListObjectsV2 API via `httpx.AsyncClient.get()` with `list-type=2` and `prefix=pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/`, parses XML response using `xml.etree.ElementTree` with namespace `http://s3.amazonaws.com/doc/2006-03-01/`, handles pagination via `NextContinuationToken`, returns list of dicts with `key`, `size`, `last_modified` per file; (b) `stream_csv_rows(s3_key: str) -> AsyncIterator[dict[str, str]]` — uses `httpx.AsyncClient.stream("GET", url)` with `aiter_bytes(chunk_size=65536)`, splits on newlines with remainder carry-forward, parses header row then yields one dict per CSV row via `csv.reader([line])` + `dict(zip(fieldnames, parsed))`, keeps memory bounded to O(64KB); (c) retry logic with exponential backoff on 5xx/timeouts wrapping the streaming download; (d) async context manager (`__aenter__`/`__aexit__`/`close()`); (e) `httpx.Timeout(connect=10.0, read=300.0)` for large file downloads. Use `follow_redirects=True` on the client.
- [x] T008 Extend `src/cloudevents_envelope.py`: parameterize `build_envelope()` to accept an optional `event_type` parameter (default `"raw.pvdaq.generation.v1"` for backwards compatibility). Feature 002 will pass `"raw.pvdaq.historical.v1"`. Also parameterize `source` to accept `"/energy-ingestion-boundary/pvdaq-historical"`.
- [x] T009 [P] Create unit tests for `csv_normalizer.py` in `tests/unit/test_csv_normalizer.py`: test `detect_timestamp_column` with `measured_on`, `timestamp`, `Date-Time`, unknown columns (returns None); test `normalize_historical_record` with valid row, missing timestamp, numeric casting, suffix stripping for both `_o_\d+` and `_\d+` patterns, empty values skipped, non-numeric values kept as string, SiteID injection.
- [x] T010 [P] Create unit tests for `oedi_historical_client.py` in `tests/unit/test_oedi_historical_client.py`: test `list_csv_files` with mocked S3 XML response (single page, paginated), 404 handling, 5xx retry; test `stream_csv_rows` with mocked streaming response, verify row-by-row yield, 404 returns empty iterator, retry on 5xx; test async context manager lifecycle. Use `respx` for HTTP mocking.
- [x] T011 [P] Update unit tests for `build_envelope` in `tests/contract/test_cloudevents_envelope.py` to verify new `event_type` and `source` parameters work correctly with both default values and explicit overrides.

**Checkpoint**: Foundation ready — CSV normalizer, S3 historical client, and parameterized envelope builder all implemented and tested. User story implementation can now begin.

---

## Phase 3: User Story 1 — Bulk Historical CSV Download (Priority: P1) MVP

**Goal**: Download all historical CSV telemetry files for 4 configured PVDAQ sites from the OEDI S3 bucket and parse them into normalized records.

**Independent Test**: Trigger the dispatcher function and verify that CSV files for each configured site are listed, work items are enqueued, and the worker function downloads and parses each file into records.

### Tests for User Story 1

- [x] T012 [P] [US1] Create integration test for dispatcher in `tests/integration/test_historical_pipeline.py`: mock `OediHistoricalClient.list_csv_files()` to return file lists for 2 sites, mock file tracking table, verify work items are enqueued to Service Bus queue for each unprocessed file, verify file tracking entities are created with status `queued`.
- [x] T013 [P] [US1] Create integration test for worker in `tests/integration/test_historical_pipeline.py`: mock `OediHistoricalClient.stream_csv_rows()` to yield sample rows, mock `csv_normalizer.normalize_historical_record()`, verify records are passed through the pipeline (validate → idempotency → emit), verify file tracking entity is updated to `completed`.

### Implementation for User Story 1

- [x] T014 [US1] Create file tracking store in `src/file_tracking_store.py`: class `FileTrackingStore` backed by Azure Table Storage with methods `get_unprocessed_files(site_id: int, discovered_files: list[dict]) -> list[dict]` (compares S3 listing against tracked files, returns new/changed ones), `mark_queued(site_id: int, s3_key: str, correlation_id: str)`, `mark_processing(site_id: int, s3_key: str)`, `mark_completed(site_id: int, s3_key: str, records_emitted: int)`, `mark_failed(site_id: int, s3_key: str)`. Uses `DefaultAzureCredential`, async context manager pattern, PartitionKey = site_id string, RowKey = SHA-256 hash of S3 key truncated to 64 chars.
- [x] T015 [US1] Implement `historical_dispatcher` function in `function_app.py`: timer-triggered on `%PVDAQ_HISTORICAL_CRON_SCHEDULE%`. For each configured site ID: (a) call `OediHistoricalClient.list_csv_files(site_id)` to discover CSV files; (b) call `FileTrackingStore.get_unprocessed_files()` to filter to new/changed files; (c) for each unprocessed file, send a work item message (JSON matching `contracts/work-item-message.json`) to the Service Bus queue; (d) call `FileTrackingStore.mark_queued()`. Log summary: sites scanned, files discovered, files enqueued. Use structured logging with correlation_id.
- [x] T016 [US1] Implement `historical_worker` function in `function_app.py`: queue-triggered on `%PVDAQ_HISTORICAL_QUEUE_NAME%`. Deserialize work item message. Call `FileTrackingStore.mark_processing()`. Stream CSV via `OediHistoricalClient.stream_csv_rows(s3_key)`. For each row, call `csv_normalizer.normalize_historical_record(row, site_id, file_name)`. Collect normalized records for downstream processing (validation/emission handled in US2). On completion, call `FileTrackingStore.mark_completed()`. On failure, call `FileTrackingStore.mark_failed()` and let the queue retry.
- [x] T017 [US1] Add unit tests for `FileTrackingStore` in `tests/unit/test_file_tracking_store.py`: test `get_unprocessed_files` (new files returned, already-completed files filtered out, changed files detected via size/lastModified), test `mark_queued`/`mark_processing`/`mark_completed`/`mark_failed` state transitions, test async context manager.

**Checkpoint**: Dispatcher discovers and enqueues CSV files, worker downloads and parses them. Records are normalized but not yet validated or emitted.

---

## Phase 4: User Story 2 — Record Validation and Emission (Priority: P2)

**Goal**: Validate each normalized CSV row against the schema and emit valid records as CloudEvents messages; dead-letter invalid records.

**Independent Test**: Provide sample normalized records (valid and invalid) and verify that valid records produce CloudEvents messages on the topic and invalid records are routed to the dead-letter queue.

### Tests for User Story 2

- [x] T018 [P] [US2] Create contract test for historical CloudEvents envelope in `tests/contract/test_historical_envelope.py`: verify envelope has `type: "raw.pvdaq.historical.v1"`, `source: "/energy-ingestion-boundary/pvdaq-historical"`, required extension attributes (`source_vendor`, `schema_version`, `correlation_id`), and `data` payload matches schema.
- [x] T019 [P] [US2] Add integration test in `tests/integration/test_historical_pipeline.py`: end-to-end flow with valid and invalid records — verify `emit_cloudevent` called for valid records with correct envelope, `emit_dead_letter` called for invalid records with rejection reason.

### Implementation for User Story 2

- [x] T020 [US2] Extend the `historical_worker` function in `function_app.py` to add the validation-and-emit pipeline: for each normalized record, call `validate_record()` from `src/schema_validator.py`. If valid: build CloudEvents envelope via `build_envelope(record, config, correlation_id, traceparent, event_type="raw.pvdaq.historical.v1", source="/energy-ingestion-boundary/pvdaq-historical")`, emit via `ServiceBusEmitter.emit_cloudevent()`. If invalid: build dead-letter message via `build_dead_letter_message()`, emit via `ServiceBusEmitter.emit_dead_letter()`. Track counts in `InvocationStats`.
- [x] T021 [US2] Wire up `ServiceBusEmitter` and config in the worker function: create emitter from config's `service_bus_fully_qualified_namespace`, use configured `service_bus_topic_name` and `dead_letter_queue_name`. Ensure emitter is properly closed via async context manager.

**Checkpoint**: Valid records are emitted as CloudEvents, invalid records are dead-lettered. The full download → normalize → validate → emit pipeline works end-to-end.

---

## Phase 5: User Story 3 — Idempotent Processing (Priority: P2)

**Goal**: Track which records have already been emitted so that re-running the ingestion does not produce duplicate messages.

**Independent Test**: Run ingestion twice for the same file and verify that the second run emits zero records (all marked as duplicates).

### Tests for User Story 3

- [x] T022 [P] [US3] Add integration test in `tests/integration/test_historical_pipeline.py`: test duplicate detection — first run emits records, second run with same data skips all as duplicates. Test pending/crash recovery — simulate crash between reserve and emit, verify re-run re-emits.

### Implementation for User Story 3

- [x] T023 [US3] Extend `src/idempotency_store.py` with an alternative `_row_key_historical` static method that composes the key as `{site_id}_{filename_stem}_{measdatetime}` (using filename stem as category discriminator per research R4). Add a `check_and_reserve_historical(site_id, file_name, measdatetime, correlation_id)` method that uses the new key format while reusing the same table and `pending`/`completed` status pattern.
- [x] T024 [US3] Integrate idempotency into the `historical_worker` function in `function_app.py`: before emitting each valid record, call `IdempotencyStore.check_and_reserve_historical()`. If `DUPLICATE`, increment `stats.number_duplicates` and skip. If `NEW` or `RETRY_EMIT`, proceed with emission. After successful emission, call `mark_completed()`. Wire up `IdempotencyStore` via async context manager in the worker.
- [x] T025 [P] [US3] Add unit tests for `check_and_reserve_historical` and `_row_key_historical` in `tests/unit/test_idempotency.py`: test key composition includes filename stem, test NEW/DUPLICATE/RETRY_EMIT flows with the historical key format.

**Checkpoint**: Re-running the ingestion for the same files produces zero duplicate emissions. Crash recovery works correctly.

---

## Phase 6: User Story 4 — Incremental File Detection (Priority: P3)

**Goal**: On subsequent runs, only process CSV files that are new or changed since the last run.

**Independent Test**: Run dispatcher once, then add a new file to the mock S3 listing, re-run, and verify only the new file is enqueued.

### Tests for User Story 4

- [x] T026 [P] [US4] Add integration test in `tests/integration/test_historical_pipeline.py`: first dispatcher run enqueues all files. Second run with same file list enqueues zero files. Third run with one new file enqueues only that file. Test size/lastModified change detection.

### Implementation for User Story 4

- [x] T027 [US4] Enhance `FileTrackingStore.get_unprocessed_files()` in `src/file_tracking_store.py`: compare `Size` and `LastModified` from S3 listing against stored values. If either changed, treat file as needing reprocessing (set status back to `queued`). Ensure the dispatcher calls this method and only enqueues files returned by it.
- [x] T028 [US4] Add edge case handling in the dispatcher: if a site's S3 listing returns zero files, log a warning and continue to the next site. If listing fails (OediAccessError), log the error and continue to the next site without crashing.

**Checkpoint**: Subsequent runs skip already-processed files. Only new or changed files are downloaded and processed.

---

## Phase 7: User Story 5 — Observability and Metrics (Priority: P3)

**Goal**: Structured logs and summary metrics emitted after each invocation for monitoring and alerting.

**Independent Test**: Trigger ingestion and verify structured log entries include correlation_id, record counts, and duration.

### Tests for User Story 5

- [x] T029 [P] [US5] Add unit test in `tests/unit/test_observability.py`: verify `InvocationStats` and `emit_invocation_metrics()` work correctly with historical source identifier. Verify structured log entries from worker include `correlation_id`, `function_name`, `vendor`, `file_name`.

### Implementation for User Story 5

- [x] T030 [US5] Add structured logging throughout `historical_dispatcher` and `historical_worker` in `function_app.py`: use `create_logger(correlation_id, vendor="PVDAQ", function_name="historical_dispatcher"|"historical_worker")`. Log: dispatcher start/end with sites scanned and files enqueued; worker start/end with file name, records retrieved/valid/invalid/emitted/duplicates, duration. Add `emit_invocation_metrics(stats)` at the end of each worker invocation.
- [x] T031 [US5] Add mapping_version warning metric: if `config.mapping_version_pvdaq == "unknown"`, call `emit_warning_metric()` at the start of each worker invocation, matching the existing feature 001 pattern.

**Checkpoint**: All invocations produce structured JSON logs with correlation IDs and metric summaries.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final validation, documentation, and cleanup

- [x] T032 [P] Run full test suite with coverage: `pytest --cov=src --cov-report=term-missing`. Ensure 100% coverage on all new modules (`csv_normalizer.py`, `oedi_historical_client.py`, `file_tracking_store.py`) and modified modules.
- [x] T033 [P] Run `ruff check .` and fix any lint violations.
- [ ] T034 Update `README.md` with feature 002 documentation: architecture diagram showing fan-out pattern, new environment variables, module descriptions.
- [ ] T035 Validate `quickstart.md` end-to-end: follow the setup instructions, start Azurite, run `func start`, verify dispatcher lists files and worker processes them.
- [ ] T036 Update `schemas/README.md` (if it exists) to document the `pvdaq-v1.json` schema reuse for historical data per constitution Principle II.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately
- **Foundational (Phase 2)**: Depends on T001 (config) from Setup — BLOCKS all user stories
- **US1 (Phase 3)**: Depends on Phase 2 completion — MVP target
- **US2 (Phase 4)**: Depends on US1 (needs worker function with record pipeline)
- **US3 (Phase 5)**: Depends on US2 (needs emission pipeline to add idempotency to)
- **US4 (Phase 6)**: Depends on US1 (needs file tracking store)
- **US5 (Phase 7)**: Depends on US1 (needs dispatcher and worker functions)
- **Polish (Phase 8)**: Depends on all desired user stories being complete

### User Story Dependencies

- **US1 (P1)**: Can start after Phase 2 — No dependencies on other stories. **MVP target.**
- **US2 (P2)**: Depends on US1 (extends the worker function with validation/emission)
- **US3 (P2)**: Depends on US2 (adds idempotency checks before emission)
- **US4 (P3)**: Can start after Phase 2 — enhances dispatcher's file detection (independent of US2/US3)
- **US5 (P3)**: Can start after US1 — adds logging/metrics to existing functions (independent of US2/US3/US4)

### Within Each User Story

- Tests written first (verify they fail before implementation)
- Models/stores before functions
- Core implementation before integration
- Story complete before moving to next priority

### Parallel Opportunities

- T002, T003, T004, T005 can all run in parallel within Phase 1
- T006 and T007 can run in parallel within Phase 2 (different files)
- T009, T010, T011 can all run in parallel within Phase 2 (test files)
- T012 and T013 can run in parallel within US1 (different test scenarios)
- T018 and T019 can run in parallel within US2 (different test files)
- US4 and US5 can run in parallel after US1 completion (independent concerns)
- T032 and T033 can run in parallel in Polish phase

---

## Parallel Example: Phase 2 (Foundational)

```bash
# Launch all independent foundational tasks together:
Task: "Create src/csv_normalizer.py" (T006)
Task: "Create src/oedi_historical_client.py" (T007)

# Once both complete, launch all tests in parallel:
Task: "Unit tests for csv_normalizer" (T009)
Task: "Unit tests for oedi_historical_client" (T010)
Task: "Contract tests for envelope" (T011)
```

## Parallel Example: User Story 1

```bash
# Launch both integration tests together:
Task: "Integration test for dispatcher" (T012)
Task: "Integration test for worker" (T013)

# Then implement sequentially: T014 (store) → T015 (dispatcher) → T016 (worker) → T017 (store tests)
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup (T001–T005)
2. Complete Phase 2: Foundational (T006–T011) — CRITICAL BLOCKER
3. Complete Phase 3: User Story 1 (T012–T017)
4. **STOP and VALIDATE**: Trigger dispatcher, verify files are listed and enqueued, verify worker downloads and parses CSV files
5. Deploy/demo if ready

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add US1 → Dispatcher + Worker download pipeline → Deploy (MVP!)
3. Add US2 → Validation + CloudEvents emission → Deploy
4. Add US3 → Idempotency → Deploy (production-ready)
5. Add US4 → Incremental detection → Deploy (efficiency)
6. Add US5 → Observability → Deploy (operational readiness)
7. Polish → 100% coverage, docs → Final release

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Feature 002 reuses shared modules from feature 001: `service_bus_emitter.py`, `observability.py`, `schema_validator.py`
- The existing `pvdaq-v1.json` schema is reused since `additionalProperties: true` accommodates varying column sets
- Idempotency key for feature 002: `{site_id}_{filename_stem}_{measdatetime}` (different from feature 001's `{site_id}_{measdatetime}`)
- Sensor suffix regex for feature 002: `r"(?:_o)?_\d+$"` (different from feature 001's `r"__\d+$"`)
