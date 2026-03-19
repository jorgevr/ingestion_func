# Research: PVDAQ Historical Dataset Ingestion (Revised Architecture)

**Date**: 2026-03-09 (revised from 2026-03-02)
**Feature**: 002-pvdaq-historical-ingestion

## R1: S3 Object Listing via HTTPS (unchanged)

**Decision**: Use the S3 ListObjectsV2 REST API over HTTPS with `xml.etree.ElementTree` for parsing.

**Rationale**: The OEDI S3 bucket is public — no AWS credentials needed. The existing `OediHistoricalClient.list_csv_files()` already implements this correctly and is reused as-is.

## R2: ADLS Gen2 Streaming Upload (NEW)

**Decision**: Use `azure-storage-file-datalake` async SDK with manual three-step upload (create → append → flush) for bounded-memory streaming.

**Rationale**: The `upload_data()` convenience method buffers internally. For files up to 870 MB, the manual pattern keeps memory at ~4 MiB steady state by streaming chunks directly from the HTTP download.

**Package**: `azure-storage-file-datalake>=12.14.0,<13.0.0` + `aiohttp` (async transport, already in requirements.txt)

**Pattern (one-pass download → hash → upload)**:
```python
from azure.storage.filedatalake.aio import DataLakeServiceClient

CHUNK_SIZE = 4 * 1024 * 1024  # 4 MiB

await file_client.create_file()
hasher = hashlib.sha256()
offset = 0
async with http_client.stream("GET", source_url) as response:
    response.raise_for_status()
    async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
        hasher.update(chunk)
        await file_client.append_data(data=chunk, offset=offset, length=len(chunk))
        offset += len(chunk)
await file_client.flush_data(offset)
file_hash = hasher.hexdigest()
```

**Key classes**: `DataLakeServiceClient` → `FileSystemClient` → `DataLakeFileClient` (all under `azure.storage.filedatalake.aio`)

**Alternatives considered**:
- `upload_data()` — buffers entire file; rejected for memory reasons.
- Azure Blob SDK — works but lacks ADLS Gen2 hierarchical namespace features.
- Two-pass (download to temp, then upload) — doubles processing time for 870 MB files.

**Operational notes**:
- `append_data` limit: 100 MiB max per call; 4 MiB recommended chunk size.
- `flush_data` must be called once at end with cumulative byte count; makes file visible.
- Memory footprint: ~4 MiB steady state regardless of total file size.

## R3: SHA-256 Streaming Hash Computation (NEW)

**Decision**: Compute SHA-256 incrementally during the streaming download+upload pass using `hashlib.sha256().update(chunk)`.

**Rationale**: Reading an 870 MB file twice would double processing time. The hasher supports incremental updates so each chunk is hashed as it flows through.

**Alternatives considered**:
- Post-upload hash via ADLS — ADLS doesn't provide server-side SHA-256.
- MD5 — cryptographically weak, inconsistent with existing codebase (IdempotencyStore uses SHA-256).

## R4: Dataset CloudEvent Envelope (NEW)

**Decision**: New event type `solar.pvdaq.dataset.available` using CloudEvents v1.0 with file-level metadata in the `data` block.

**Rationale**: The existing `raw.pvdaq.*` prefix is for per-row events. Dataset events are a higher-level fact ("a file is available"), so a `solar.pvdaq.dataset.*` namespace distinguishes the granularity.

**Envelope example**:
```json
{
  "specversion": "1.0",
  "type": "solar.pvdaq.dataset.available",
  "source": "/energy-ingestion-boundary/pvdaq",
  "id": "<uuid>",
  "time": "<iso-8601>",
  "datacontenttype": "application/json",
  "tenant_id": "<from-config>",
  "source_vendor": "PVDAQ",
  "schema_version": "v1",
  "correlation_id": "<uuid, same as ingestion_id>",
  "ingestion_timestamp": "<iso-8601>",
  "traceparent": "<w3c-trace>",
  "data": {
    "site_id": 9068,
    "category": "irradiance",
    "file_format": "csv",
    "storage_path": "abfss://raw@{account}.dfs.core.windows.net/pvdaq/site_id=9068/category=irradiance/9068_irradiance_data.csv",
    "ingestion_id": "<uuid>",
    "source_url": "https://oedi-data-lake.s3.amazonaws.com/pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_irradiance_data.csv",
    "file_size": 524288,
    "file_hash": "<sha256-hex>"
  }
}
```

**Extension attribute notes**:
- `mapping_version` is omitted — no field mapping occurs at dataset level. The existing envelope builder needs a parameter to make it optional.
- `correlation_id` should equal `ingestion_id` for lineage tracing.
- Service Bus routing: `subject` = `"solar.pvdaq.dataset.available"`, `application_properties` carry `source_vendor` and `schema_version` for subscription filtering.

## R5: Category Extraction from Filename (unchanged)

**Decision**: Use the full S3 key basename (minus `.csv` extension) as the category/file identifier. For sites with standard naming (`{site_id}_{category}_data.csv`), also extract a human-readable category.

**Rationale**: File naming varies across sites (see original R4). The full filename is universally unique and sufficient for idempotency and ADLS path construction.

**Category extraction regex** (for standard files): Strip `{site_id}_` prefix and `_data` suffix from basename. Fallback: use full basename.

## R6: File Tracking Store Extensions (NEW)

**Decision**: Extend the existing `PvdaqFileTracking` Azure Table entity with new columns for dataset metadata.

**New fields added to the existing entity**:
- `StoragePath`: ADLS Gen2 file path (string)
- `FileHash`: SHA-256 hex digest (string, 64 chars)
- `IngestionId`: UUID (string)
- `SourceUrl`: Original S3 URL (string)
- `IngestionTime`: ISO-8601 timestamp (string)

**Rationale**: Avoids a separate metadata store. The existing entity already has PartitionKey (site_id) and RowKey (SHA-256 of S3 key), plus Status, Size, LastModified, etc. Adding metadata fields to `mark_completed()` is minimal code change.

## R7: Existing Code Impact (NEW)

**What changes**:
| Module | Change |
| --- | --- |
| `function_app.py` (historical_worker) | Remove row iteration, record_pipeline, csv_normalizer. Replace with: download → stream to ADLS → hash → update tracking → emit dataset event |
| `function_app.py` (historical_dispatcher) | Minimal — still lists files, filters, enqueues work items |
| `src/file_tracking_store.py` | Add metadata fields to `mark_completed()` |
| `src/adls_store.py` (NEW) | ADLS Gen2 streaming upload + hash computation |
| `src/config.py` | Add `adls_account_url`, `adls_container_name` to HistoricalConfig |
| `src/cloudevents_envelope.py` | Add dataset event builder (or parameterize existing) |
| `src/observability.py` | Add `DatasetIngestionStats` or update metric names |
| `requirements.txt` | Add `azure-storage-file-datalake>=12.14.0,<13.0.0` |

**What stays unchanged**: Feature 001 functions, `oedi_historical_client.py` (list_csv_files reused), `service_bus_emitter.py`, `http_retry.py`, `schema_validator.py`.

## R8: Event Type Registration (updated)

**Decision**: Register `solar.pvdaq.dataset.available` in `topics.md` manifest per Constitution VII.

**Naming**: Departs from the `raw.{vendor}.{data_category}.v{major}` convention since this is a dataset-available notification, not a raw data event. The `solar.` prefix indicates a domain-level event.

**Topic**: Same `raw-energy-events` topic — consumers filter by `subject` or `type`.

---

## R9: ADLS Path — Gate Failure Resolution (2026-03-19)

**Decision**: Path MUST be `raw/pvdaq/site_id={site_id}/year={year}/month={month}/{file_name}`.
`year` and `month` derived in UTC from S3 `LastModified` timestamp; fall back to ingestion date.

**Current state (bug)**: `function_app.py:341` builds `pvdaq/site_id={site_id}/category={category}/{file_name}`.
- Missing `raw/` prefix — violates Constitution VIII and spec FR-004.
- Uses `category` partition instead of `year/month` — diverges from the standard bronze layout.

**Fix strategy**:
1. Add `last_modified` to work-item message in dispatcher (already available from S3 listing).
2. In worker, parse `last_modified` with UTC normalisation:
   ```python
   dt = datetime.fromisoformat(last_modified.replace("Z", "+00:00")).astimezone(timezone.utc)
   adls_path = f"raw/pvdaq/site_id={site_id}/year={dt.year}/month={dt.month:02d}/{file_name}"
   ```
3. Fallback to ingestion date when `last_modified` is absent.
4. Update `contracts/work-item-message.json` — add optional `last_modified` field.
5. Fix integration test assertion in `test_worker_deterministic_adls_path`.

## R10: Work-Item Schema Validation Gap (Constitution II) (2026-03-19)

**Decision**: Validate incoming work-item queue message in `historical_worker` using `schema_validator.py`.

**Current state**: Worker does raw `json.loads` then accesses fields directly — no validation.

**Fix**: At the top of `historical_worker`, before `mark_processing`, validate the parsed dict
against `work-item-message.json`. On failure, dead-letter and return (no retry for malformed messages).

Note: `quickstart.md` listed `src/schema_validator.py` as unused by feature 002 — this is now incorrect.

## R11: Local Emulation Switching (2026-03-19)

**Decision**:
- **ADLS Azurite**: Set `ADLS_ACCOUNT_URL=http://127.0.0.1:10000/devstoreaccount1` in
  `local.settings.json`. No code change needed — `AdlsStore` accepts `account_url` from config.
  Azurite ≥3.22.0 supports the DFS endpoint used by `azure-storage-file-datalake`.
- **Service Bus → Azurite Queue**: Introduce `AzuriteQueueEmitter` using `azure-storage-queue`
  SDK. Activated when `STORAGE_EMULATOR=true`. Both emitter types expose the same interface
  (`emit_cloudevent`, `emit_dead_letter`, `send_queue_message`). Factory in `function_app.py`
  selects the correct type based on env var.
