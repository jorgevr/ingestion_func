# Schemas

Validation schemas for the energy-ingestion-boundary service.

## Naming Convention

```
{vendor}-v{major}.json
```

Example: `pvdaq-v1.json` — PVDAQ photovoltaic telemetry schema, version 1.

## Schema Discovery

All schemas in this directory are loaded at module init time by the
corresponding validator module in `src/`. The validator resolves schemas
relative to its own file path:

```python
Path(__file__).resolve().parent.parent / "schemas" / "pvdaq-v1.json"
```

## Authoritative Source

**`pvdaq-v1.json`**: the authoritative contract lives in
`specs/001-pvdaq-ingestion/contracts/`. This copy is what's deployed with the
function app. When updating it:

1. Update the contract in `specs/.../contracts/`
2. Copy to `schemas/`
3. Bump the version suffix if breaking changes are introduced
4. Register new event types in `topics.md` per constitution Principle VII

**`work-item.v1.json`**: this repo (`schemas/`) *is* the authoritative
source — it is service-internal (the dispatcher → worker Service Bus
message), never crosses a boundary, and is therefore not part of the root
`contracts/` registry either (docs/contracts.md §5). There is no copy under
`specs/` to keep in sync — update it here directly.

**Every schema loaded at runtime must live under this directory.** The
Dockerfile copies `schemas/` into the image but never `specs/` or `docs/`
(see `Dockerfile`'s `COPY` lines) — a schema path built from `specs/...` or
`docs/...` only fails inside the container, never in local dev where the
whole repo is on disk. `tests/unit/test_schema_packaging.py` guards this.

## Current Schemas

| File | Scope | Version | Required Fields |
|------|-------|---------|-----------------|
| `pvdaq-v1.json` | PVDAQ telemetry record (per-row) | v1 | `SiteID` (integer), `measdatetime` (string, ISO-8601) |
| `work-item.v1.json` | Historical dispatcher → worker message (internal) | v1 | `site_id`, `s3_key`, `file_name`, `correlation_id`, `enqueued_at` |

## Schema Usage by Feature

`pvdaq-v1.json` is used exclusively by feature 001 (daily polling) for per-record
validation. Feature 002 (historical ingestion) operates at the **dataset level**:
it streams CSV files directly from S3 to ADLS Gen2 without per-row parsing, so no
record-level schema is applied to the CSV itself — but the work item message that
drives each worker invocation is validated against `work-item.v1.json`. The
CloudEvent data block contract is defined in
`specs/002-pvdaq-historical-ingestion/contracts/dataset-event.json`.

| Feature | Record/Message Schema | CloudEvent `type` |
| ------- | ---------------------- | ----------------- |
| 001 - daily polling | `pvdaq-v1.json` | `raw.pvdaq.generation.v1` |
| 002 - historical | `work-item.v1.json` (queue message only — no CSV row schema) | `solar.pvdaq.dataset.available` |


