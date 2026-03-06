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

## Schema Reuse

Per constitution Principle II (reuse before new), `pvdaq-v1.json` is shared
by both feature 001 (daily polling) and feature 002 (historical ingestion).
The historical pipeline normalises CSV columns to the same `SiteID` /
`measdatetime` required fields, so no additional schema is needed. The two
features are distinguished by CloudEvents metadata:

| Feature | `type` | `source` |
| ------- | ------ | -------- |
| 001 - daily polling | `raw.pvdaq.v1` | `/energy-ingestion-boundary/pvdaq` |
| 002 - historical | `raw.pvdaq.historical.v1` | `/energy-ingestion-boundary/pvdaq-historical` |


