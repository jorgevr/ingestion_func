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

The authoritative contract schemas live in
`specs/001-pvdaq-ingestion/contracts/`. Files in this directory are
copies deployed with the function app. When updating a schema:

1. Update the contract in `specs/.../contracts/`
2. Copy to `schemas/`
3. Bump the version suffix if breaking changes are introduced
4. Register new event types in `topics.md` per constitution Principle VII

## Current Schemas

| File | Vendor | Version | Required Fields |
|------|--------|---------|-----------------|
| `pvdaq-v1.json` | PVDAQ | v1 | `SiteID` (integer), `measdatetime` (string, ISO-8601) |

## Schema Usage by Feature

`pvdaq-v1.json` is used exclusively by feature 001 (daily polling) for per-record
validation. Feature 002 (historical ingestion) operates at the **dataset level**:
it streams CSV files directly from S3 to ADLS Gen2 without per-row parsing, so no
record-level schema is applied. Its CloudEvent data block contract is defined in
`specs/002-pvdaq-historical-ingestion/contracts/dataset-event.json`.

| Feature | Record Schema | CloudEvent `type` |
| ------- | ------------- | ----------------- |
| 001 - daily polling | `pvdaq-v1.json` | `raw.pvdaq.generation.v1` |
| 002 - historical | _(none — dataset-level)_ | `solar.pvdaq.dataset.available` |


