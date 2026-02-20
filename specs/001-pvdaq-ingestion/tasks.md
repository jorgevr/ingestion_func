# Tasks: PVDAQ Ingestion

**Input**: Design documents from `/specs/001-pvdaq-ingestion/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Included — constitution mandates schema validation unit tests and emission contract tests before merge. Each user story has an Independent Test criterion in spec.md.

**Organization**: Tasks grouped by user story. Implementation order: US2 → US3 → US1 → US4 → US5 (all P1 stories first, ordered by dependency flow: validation and emission are building blocks that US1 orchestrates).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1–US5)
- Include exact file paths in descriptions

---

## Phase 1: Setup (Project Initialization)

**Purpose**: Create the Azure Functions Python v2 project skeleton and install dependencies.

- [ ] T001 Create project directory structure: `src/`, `schemas/`, `tests/contract/`, `tests/integration/`, `tests/unit/` per plan.md
- [ ] T002 Create `requirements.txt` with pinned dependencies: azure-functions, azure-servicebus, azure-data-tables, azure-identity, httpx, jsonschema. Add dev dependencies: pytest, pytest-asyncio, respx, ruff (per research.md R8)
- [ ] T003 [P] Create `host.json` with: `version: "2.0"`, `functionTimeout: "00:10:00"`, extension bundle `[4.0.0, 5.0.0)`, Service Bus `clientRetryOptions` (exponential, maxRetries: 3), and structured logging config for Application Insights (per research.md R7)
- [ ] T004 [P] Create `local.settings.json.template` with all FR-011 settings (PVDAQ_API_BASE_URL, PVDAQ_API_KEY, PVDAQ_SITE_IDS, PVDAQ_LOOKBACK_HOURS, PVDAQ_CRON_SCHEDULE, SERVICE_BUS_TOPIC_NAME, DEAD_LETTER_QUEUE_NAME, ServiceBusConnection__fullyQualifiedNamespace, IDEMPOTENCY_TABLE_NAME, TableStorageConnection__tableServiceUri, TENANT_ID, MAPPING_VERSION_PVDAQ, SCHEMA_VERSION_PVDAQ) per data-model.md Configuration Entities table
- [ ] T005 [P] Create `.funcignore` excluding tests/, specs/, .venv/, __pycache__/, .git/ from deployment
- [ ] T005a [P] Create `topics.md` manifest at repository root — register event type `raw.pvdaq.generation.v1` with topic `raw-energy-events`, owner `pvdaq_ingest`, and schema reference `pvdaq-v1.json` per constitution Principle VII ("MUST be registered before first use")

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core modules that ALL user stories depend on. MUST complete before any user story work begins.

**CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T006 Create `src/__init__.py` (empty module init)
- [ ] T007 Create `src/config.py` — externalized configuration loader that reads all FR-011 settings from environment variables via `os.environ`. Must parse `PVDAQ_SITE_IDS` as comma-separated integer list, `PVDAQ_LOOKBACK_HOURS` as int, and provide typed access to all config values. Raise clear errors on missing required settings
- [ ] T008 [P] Create `src/observability.py` — structured logging setup: configure Python `logging` to emit JSON-formatted log entries with `correlation_id`, `function_name`, and `vendor` fields. Provide a `create_logger(correlation_id, vendor="PVDAQ")` factory function. Defer custom metrics to US5
- [ ] T009 [P] Create `schemas/pvdaq-v1.json` — copy from `specs/001-pvdaq-ingestion/contracts/pvdaq-v1.json`. This is the authoritative validation schema requiring `SiteID` (integer) and `measdatetime` (string, ISO-8601 pattern), with `additionalProperties: true`
- [ ] T010 [P] Create `src/service_bus_emitter.py` — Service Bus SDK sender using `azure-servicebus` with `DefaultAzureCredential`. Expose two methods: `emit_event(topic_name, message_body, content_type, subject, application_properties)` and `emit_dead_letter(queue_name, message_body)`. Accept `ServiceBusSender` via constructor for testability (per research.md R3)
- [ ] T011 Create `tests/conftest.py` — shared pytest fixtures: sample valid PVDAQ record dict, sample invalid record (missing SiteID), mock config object, mock ServiceBusSender, correlation_id fixture
- [ ] T012 [P] Create `tests/unit/test_config.py` — test config loader: valid env → correct typed values; missing required setting → clear error; PVDAQ_SITE_IDS parsing (single, multiple, empty)

**Checkpoint**: Foundation ready — user story implementation can begin.

---

## Phase 3: User Story 2 — Schema Validation Gate (Priority: P1)

**Goal**: Every record is validated against `pvdaq-v1.json` before further processing. Invalid records are rejected, dead-lettered with error metadata, and never propagated downstream.

**Independent Test**: Submit a deliberately malformed payload → verify it is rejected, dead-lettered with error details, and logged. Submit a valid payload → verify it passes through.

### Tests for User Story 2

> **Write tests FIRST, ensure they FAIL before implementation**

- [ ] T013 [P] [US2] Write schema validation unit tests in `tests/unit/test_schema_validation.py` — test cases: valid record passes, missing `SiteID` fails, missing `measdatetime` fails, wrong type for `SiteID` (string instead of int) fails, extra fields are allowed (`additionalProperties: true`), multiple errors collected per record
- [ ] T014 [P] [US2] Write contract test for invalid payload rejection in `tests/contract/test_invalid_payload.py` — assert that a malformed PVDAQ record produces a dead-letter message matching the `contracts/dead-letter-message.json` schema (original_payload preserved, error_details populated, correlation_id present)

### Implementation for User Story 2

- [ ] T015 [US2] Implement schema validation logic in `src/schema_validator.py` — load schema from `schemas/pvdaq-v1.json` at module level, expose `validate_record(record: dict) -> tuple[bool, list[dict]]` returning (is_valid, error_details). Use `jsonschema.validate()` with `jsonschema.Draft202012Validator`. Collect all errors (not just first) via `validator.iter_errors()`
- [ ] T016 [US2] Add dead-letter message construction function to `src/service_bus_emitter.py` — `build_dead_letter_message(original_payload, error_details, correlation_id, site_id, schema_version)` returns a dict matching `contracts/dead-letter-message.json` schema. Wire into `emit_dead_letter()` with `content_type="application/json"` and `subject="validation_failure"`
- [ ] T017 [US2] Write dead-letter routing integration test in `tests/integration/test_dead_letter.py` — mock ServiceBusSender, submit invalid record through validator → assert `emit_dead_letter()` called with correctly structured message, correct queue name from config

**Checkpoint**: Schema validation gate independently testable. Invalid records rejected + dead-lettered; valid records pass through.

---

## Phase 4: User Story 3 — Metadata Enrichment & Event Emission (Priority: P1)

**Goal**: Each validated record is enriched with a CloudEvents-compatible envelope and emitted to the Service Bus topic with correct metadata.

**Independent Test**: Provide a valid PVDAQ record → assert emitted Service Bus message is a valid CloudEvents envelope with correct core fields, extension attributes, and unmodified original payload under `data`.

### Tests for User Story 3

> **Write tests FIRST, ensure they FAIL before implementation**

- [ ] T018 [P] [US3] Write CloudEvents envelope contract test in `tests/contract/test_cloudevents_envelope.py` — validate a constructed envelope against `contracts/cloudevents-envelope.json` schema. Assert: `specversion="1.0"`, `type="raw.pvdaq.generation.v1"`, `source="/energy-ingestion-boundary/pvdaq"`, `datacontenttype="application/json"`, all extension attributes present (`tenant_id`, `source_vendor`, `schema_version`, `mapping_version`, `correlation_id`, `ingestion_timestamp`, `traceparent`), `data` equals original payload
- [ ] T019 [P] [US3] Write metadata enrichment unit test in `tests/unit/test_metadata_enrichment.py` — test: envelope has all required fields, `mapping_version` defaults to `"unknown"` when env var missing, `correlation_id` is valid UUID, `ingestion_timestamp` is UTC ISO-8601, original payload is not mutated, `id` is unique UUID per call

### Implementation for User Story 3

- [ ] T020 [US3] Implement CloudEvents envelope builder in `src/cloudevents_envelope.py` — expose `build_envelope(record, config, correlation_id, traceparent) -> dict` that constructs a CloudEvents dict per FR-006: core fields (`specversion`, `type`, `source`, `id`, `time`, `datacontenttype`) + extension attributes (`tenant_id`, `source_vendor`, `schema_version`, `mapping_version`, `correlation_id`, `ingestion_timestamp`, `traceparent`) + `data` = unmodified record. Read `mapping_version` from config with `"unknown"` sentinel fallback. Extract `traceparent` from invocation context or generate new trace ID per FR-009a
- [ ] T021 [US3] Wire CloudEvents topic emission into `src/service_bus_emitter.py` — add `emit_cloudevent(topic_name, envelope_dict)` that serializes to JSON and sends via `ServiceBusMessage` with `content_type="application/cloudevents+json"`, `subject=envelope["type"]`, `application_properties={"source_vendor": ..., "schema_version": ...}` per data-model.md §Ingestion Event
- [ ] T022 [US3] Write valid payload emission contract test in `tests/contract/test_valid_payload.py` — end-to-end: valid PVDAQ record → build envelope → validate against CloudEvents schema → mock ServiceBusSender → assert `send_messages()` called with correct `content_type` and `subject`

**Checkpoint**: Enrichment + emission independently testable. Valid records produce constitutionally compliant CloudEvents messages on the topic.

---

## Phase 5: User Story 1 — Scheduled Telemetry Retrieval (Priority: P1) MVP

**Goal**: The system retrieves PVDAQ telemetry on a CRON schedule, validates, enriches, and emits raw events end-to-end. This is the minimum viable slice.

**Independent Test**: Trigger the function with a mock PVDAQ API response → verify correctly shaped messages appear on the Service Bus topic.

### Tests for User Story 1

> **Write tests FIRST, ensure they FAIL before implementation**

- [ ] T023 [P] [US1] Write PVDAQ API client unit tests in `tests/unit/test_pvdaq_access.py` — test: successful response parsed into list of record dicts, empty response returns empty list, HTTP 429 triggers retry (mock `Retry-After` header), HTTP 500 retries up to 3 times with backoff, network timeout raises after retries exhausted, API key injected into request headers
- [ ] T024 [P] [US1] Write mock PVDAQ API integration test in `tests/integration/test_pvdaq_api.py` — use `respx` to mock `https://developer.nrel.gov/api/pvdaq/v3/site_data` responses. Test: multi-site sequential polling, per-site time window calculation from `PVDAQ_LOOKBACK_HOURS`, partial site failure (one site 500, others succeed per FR-012)

### Implementation for User Story 1

- [ ] T025 [US1] Implement PVDAQ API client in `src/pvdaq_access.py` — thin `httpx`-based client wrapping NREL API (per research.md R1). Constructor takes `base_url`, `api_key`, `httpx.AsyncClient` (for testability). Expose `async fetch_site_data(site_id, start_date, end_date) -> list[dict]`. Implement: API key via `X-Api-Key` header, exponential backoff (3 retries), `Retry-After` header respect, structured error logging
- [ ] T026 [US1] Implement timer trigger and pipeline orchestration in `function_app.py` — register `@app.timer_trigger(schedule="%PVDAQ_CRON_SCHEDULE%", arg_name="timer", run_on_startup=False)`. Pipeline per invocation: generate `correlation_id` → load config → for each site_id (sequential): fetch records → for each record: validate (US2) → if valid: enrich (US3) + emit to topic → if invalid: dead-letter (US2). Handle partial failures per FR-012 (continue on per-record and per-site errors). Log invocation summary
- [ ] T027 [US1] Write end-to-end Service Bus emission integration test in `tests/integration/test_service_bus_emission.py` — mock PVDAQ API (via respx) + mock ServiceBusSender. Trigger pipeline → assert: correct number of `emit_cloudevent()` calls, correct number of `emit_dead_letter()` calls for invalid records, no emission for empty API response

**Checkpoint**: Full pipeline functional end-to-end. This is the MVP — retrieval + validation + enrichment + emission all working together. STOP AND VALIDATE.

---

## Phase 6: User Story 4 — Idempotent Emission (Priority: P2)

**Goal**: Duplicate records (same site ID + timestamp) are emitted at most once within the idempotency window, preventing duplicate events on the backbone.

**Independent Test**: Submit the same PVDAQ record twice → verify only one event is emitted; second invocation succeeds silently.

**Depends on**: US1 (pipeline must exist to integrate idempotency into)

### Tests for User Story 4

> **Write tests FIRST, ensure they FAIL before implementation**

- [ ] T028 [P] [US4] Write idempotency store unit tests in `tests/unit/test_idempotency.py` — test: new record → `create_entity(Status="pending")` called → returns `is_new=True`, duplicate record (ResourceExistsError with Status="completed") → returns `is_new=False`, pending record (previous crash) → returns `should_retry=True`, `mark_completed()` calls `update_entity(Status="completed")`, composite key format is `"{SiteID}_{measdatetime_iso}"`, PartitionKey is date-based `"YYYY-MM-DD"`

### Implementation for User Story 4

- [ ] T029 [US4] Implement idempotency store in `src/idempotency_store.py` — wraps `azure.data.tables.TableClient` with `DefaultAzureCredential`. Expose: `check_and_reserve(site_id, measdatetime, correlation_id) -> IdempotencyResult` (enum: NEW, DUPLICATE, RETRY_EMIT), `mark_completed(partition_key, row_key)`. Use `create_entity()` conditional insert per research.md R5. PartitionKey = date from measdatetime, RowKey = `"{SiteID}_{measdatetime}"`
- [ ] T030 [US4] Integrate idempotency check into pipeline in `function_app.py` — insert idempotency check after validation, before emission: if NEW → emit + mark_completed; if DUPLICATE → skip + log; if RETRY_EMIT → re-emit + mark_completed. Fail-closed: if idempotency store unavailable, fail the invocation (per spec Edge Case 3)
- [ ] T031 [US4] Write idempotency integration test in `tests/integration/test_idempotency_integration.py` — mock TableClient + ServiceBusSender. Test: first submission → emitted + idempotency record created, second submission (same key) → not emitted + logged as duplicate, pending record (simulated crash) → re-emitted + marked completed
- [ ] T031a [US4] Implement TTL cleanup in `src/idempotency_store.py` — add `cleanup_expired(ttl_days: int)` method that queries partitions older than `ttl_days` and deletes them in batches via `submit_transaction()`. Document that a separate daily timer function (or manual invocation) should call this. Defer the actual timer trigger registration to a follow-on task if out of scope

**Checkpoint**: Deduplication active. Duplicate records silently suppressed; crash recovery handles pending records.

---

## Phase 7: User Story 5 — Structured Observability (Priority: P2)

**Goal**: Every invocation emits structured telemetry (metrics and logs) for monitoring ingestion health and troubleshooting.

**Independent Test**: Trigger the function → assert structured log entries and custom metrics appear with expected fields and values.

**Depends on**: US1 (pipeline must exist to instrument)

### Tests for User Story 5

> **Write tests FIRST, ensure they FAIL before implementation**

- [ ] T032 [P] [US5] Write observability unit tests in `tests/unit/test_observability.py` — test: `emit_invocation_metrics()` produces dict with all FR-009 fields (`source`, `number_of_records_retrieved`, `number_valid`, `number_invalid`, `number_emitted`, `duration_ms`, `correlation_id`), warning metric emitted when `mapping_version="unknown"`, structured log entries include `correlation_id`, `function_name`, `vendor`

### Implementation for User Story 5

- [ ] T033 [US5] Enhance `src/observability.py` with custom metrics — add `emit_invocation_metrics(stats: InvocationStats)` that logs a structured JSON summary with all FR-009 telemetry fields. Add `emit_warning_metric(metric_name, details)` for mapping_version sentinel warning. Add `InvocationStats` dataclass to accumulate counts during pipeline execution
- [ ] T034 [US5] Wire telemetry into pipeline in `function_app.py` — instantiate `InvocationStats` at invocation start, increment counters (retrieved, valid, invalid, emitted) as pipeline processes records, call `emit_invocation_metrics()` at invocation end. Add duration_ms tracking via `time.monotonic()`. Emit mapping_version warning if sentinel detected
- [ ] T035 [US5] Add structured logging to all `src/` modules — ensure `pvdaq_access.py`, `schema_validator.py`, `service_bus_emitter.py`, and `idempotency_store.py` use the structured logger from `observability.py` with consistent `correlation_id` threading

**Checkpoint**: Full observability active. Every invocation produces structured telemetry per FR-009. Mapping version warnings emitted per FR-006.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Constitution compliance artifacts, configuration tuning, and final validation.

- [ ] T036 [P] Create `schemas/README.md` — document schema discovery mechanism, naming convention `{vendor}-v{major}.json`, and artifact publishing guidance per constitution Principle II
- [ ] T036a [P] Create alert rule definitions document or Bicep template for Application Insights alerts: validation failure spike (>10% of records), emission failure (after retry exhaustion), abnormal processing latency — per FR-009b and constitution Principle V
- [ ] T038 Review and finalize `host.json` — validate `functionTimeout` accommodates 10-site sequential polling, Service Bus `clientRetryOptions` align with FR-003 retry semantics, logging level appropriate for production
- [ ] T039 Run full test suite (`pytest`) and lint pass (`ruff check .`) — all tests green, zero lint violations
- [ ] T040 Validate quickstart.md — walk through local setup steps, verify `func start` launches successfully, verify manual trigger via admin endpoint produces expected output against mock/local services

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — start immediately
- **Foundational (Phase 2)**: Depends on Setup — **BLOCKS all user stories**
- **US2 (Phase 3)**: Depends on Foundational — builds validation + dead-letter
- **US3 (Phase 4)**: Depends on Foundational — builds enrichment + emission
- **US1 (Phase 5)**: Depends on US2 + US3 — orchestrates full pipeline (MVP)
- **US4 (Phase 6)**: Depends on US1 — adds idempotency to existing pipeline
- **US5 (Phase 7)**: Depends on US1 — instruments existing pipeline
- **Polish (Phase 8)**: Depends on all user stories complete

### User Story Dependencies

```
Phase 1: Setup
    │
Phase 2: Foundational
    │
    ├── Phase 3: US2 (Validation) ──┐
    │                                ├── Phase 5: US1 (Retrieval) 🎯 MVP
    └── Phase 4: US3 (Enrichment) ──┘         │
                                          ┌────┴────┐
                                   Phase 6: US4   Phase 7: US5
                                   (Idempotency)  (Observability)
                                          │           │
                                          └─────┬─────┘
                                                │
                                         Phase 8: Polish
```

### Within Each User Story

1. Tests written FIRST (marked [P] where independent)
2. Tests must FAIL before implementation begins
3. Implementation tasks in dependency order
4. Story checkpoint validates independent testability

### Parallel Opportunities

**Phase 1**: T003, T004, T005, T005a can run in parallel
**Phase 2**: T008, T009, T010, T012 can run in parallel (after T006, T007)
**Phase 3 + Phase 4**: US2 and US3 can run in parallel (both depend only on Foundational)
**Phase 6 + Phase 7**: US4 and US5 can run in parallel (both depend on US1)
**Phase 8**: T036 and T036a can run in parallel

---

## Parallel Example: US2 + US3 (after Foundational)

```
# These two phases can execute in parallel since they touch different files:

# US2 (Phase 3):
Task: T013 [US2] tests/unit/test_schema_validation.py
Task: T014 [US2] tests/contract/test_invalid_payload.py
Task: T015 [US2] src/schema_validator.py
Task: T016 [US2] src/service_bus_emitter.py (dead-letter addition)

# US3 (Phase 4) — simultaneously:
Task: T018 [US3] tests/contract/test_cloudevents_envelope.py
Task: T019 [US3] tests/unit/test_metadata_enrichment.py
Task: T020 [US3] src/cloudevents_envelope.py
Task: T021 [US3] src/service_bus_emitter.py (topic emission addition)

# Note: T016 and T021 both modify service_bus_emitter.py — if truly parallel,
# coordinate or merge. Otherwise run US2 first (T016), then US3 (T021).
```

---

## Implementation Strategy

### MVP First (US1 = Phase 5 Checkpoint)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL — blocks all stories)
3. Complete Phase 3: US2 (Schema Validation)
4. Complete Phase 4: US3 (Enrichment & Emission)
5. Complete Phase 5: US1 (Scheduled Retrieval — wires everything together)
6. **STOP AND VALIDATE**: Full pipeline works end-to-end. This is the MVP.

### Incremental Delivery

1. Setup + Foundational → skeleton ready
2. US2 + US3 → building blocks tested independently
3. US1 → full pipeline functional (MVP!)
4. US4 → deduplication active
5. US5 → production-ready observability
6. Polish → constitution compliance artifacts complete

---

## Notes

- [P] tasks = different files, no dependencies on incomplete tasks
- [Story] label maps task to specific user story for traceability
- Constitution mandates tests: schema validation unit tests + emission contract tests before merge
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- `service_bus_emitter.py` is modified by both US2 (T016) and US3 (T021) — sequence if not parallelizing
