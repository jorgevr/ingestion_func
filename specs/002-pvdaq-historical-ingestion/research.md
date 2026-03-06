# Research: PVDAQ Historical Data Ingestion

**Date**: 2026-03-02
**Feature**: 002-pvdaq-historical-ingestion

## R1: S3 Object Listing via HTTPS

**Decision**: Use the S3 ListObjectsV2 REST API over HTTPS with `xml.etree.ElementTree` for parsing.

**Rationale**: The OEDI S3 bucket is public — no AWS credentials needed. A plain `GET` with `list-type=2` and `prefix` returns XML listing all objects. httpx handles this with standard `client.get()`. No additional dependencies required.

**URL pattern**:
```
GET https://oedi-data-lake.s3.amazonaws.com/?list-type=2&prefix=pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/
```

**XML namespace**: `http://s3.amazonaws.com/doc/2006-03-01/` — must be used in all `ElementTree.find()` calls.

**Pagination**: When `<IsTruncated>true</IsTruncated>`, pass the `<NextContinuationToken>` as `continuation-token` query param in the next request. Max 1000 keys per page.

**Key fields per object**: `<Key>` (S3 path), `<Size>` (bytes), `<LastModified>` (ISO-8601).

**Alternatives considered**:
- boto3: Would add a new dependency and requires AWS SDK; unnecessary for public bucket reads.
- Manual URL construction: Fragile; the ListObjectsV2 API is the standard approach.

**Gotchas**:
- XML namespace is mandatory in all tag lookups.
- `list-type=2` is required; without it, legacy v1 API uses `<Marker>` for pagination.
- Region redirects may occur; set `follow_redirects=True` on httpx client.

## R2: Streaming CSV Download and Parsing

**Decision**: Use `httpx.AsyncClient.stream("GET", url)` with `aiter_bytes()` for chunked download, combined with a line-by-line CSV parser that maintains bounded memory.

**Rationale**: CSV files range from 7 MB to 870 MB. Loading the entire response into memory (as the feature 001 client does) would consume 2-3x the file size in RAM, exceeding the Azure Function 1.5 GB memory limit for the largest files.

**Pattern**: Streaming byte chunks → split on `\n` → carry partial lines → `csv.reader([line])` per complete line → `dict(zip(fieldnames, parsed))`.

**Memory budget**: O(chunk_size + one_line_length) ≈ 128 KB at any given time.

**Timeout configuration**: Use `httpx.Timeout(connect=10.0, read=300.0)` — large files may take minutes to download at typical S3 throughput.

**Retry strategy**: Retry wraps the entire streaming download (not individual chunks). On 5xx or timeout, retry from scratch with exponential backoff. Idempotency store handles already-emitted records on retry.

**Alternatives considered**:
- `response.text` + `io.StringIO`: Current feature 001 approach; works for small files but impossible for 870 MB.
- `aiter_lines()`: httpx provides this but it buffers internally with less control.
- Temporary file download then parse: Adds I/O latency and disk usage; unnecessary when streaming works.

**Gotchas**:
- `csv.DictReader` cannot consume async iterators directly; manual line-by-line feeding required.
- Embedded newlines in quoted CSV fields would break line splitting, but PVDAQ telemetry data is numeric-only so this is not expected.
- `httpx.ReadTimeout` can fire mid-stream; the retry wrapper must handle this.

## R3: CSV Column Names and Normalization

**Decision**: The timestamp column is `measured_on` across all confirmed files. No `system_id` column exists in 2023-solar-data-prize CSVs — the site ID must be injected from the S3 path/filename. Sensor suffix regex must be broadened.

**Rationale**: Research examined actual CSV headers from sites 2107 and 9069 across multiple categories. All use `measured_on`. No file contained a `system_id` column — the site ID is encoded only in the folder path (`{site_id}_OEDI`).

**Confirmed CSV header patterns**:

| Site | Category | Headers |
| --- | --- | --- |
| 2107 | meter_15m | `measured_on,meter_revenue_grade_ac_output_meter_149578` |
| 2107 | environment | `measured_on,ambient_temperature_o_149575,wind_speed_o_149576,...` |
| 2107 | irradiance | `measured_on,poa_irradiance_o_149574` |
| 9069 | combiner | `measured_on,combiner_dc_input_01.01.01_dc_current_string_01_(a)_o_151942` |

**Critical difference from feature 001**:
- Feature 001 CSVs (`pvdaq/csv/pvdata/`) have both `system_id` and `measured_on` columns.
- Feature 002 CSVs (`pvdaq/2023-solar-data-prize/`) have only `measured_on` — no `system_id`.
- The new `csv_normalizer.py` must inject `SiteID` from the S3 path, not from the CSV row.

**Sensor suffix patterns**:
- Feature 001: `__\d+` (double underscore + digits, e.g., `dc_power__346`)
- Feature 002: `_o_\d+` (e.g., `ambient_temperature_o_149575`) or `_\d+` (e.g., `meter_..._149578`)
- New regex: `r"(?:_o)?_\d+$"` handles both patterns.

**Timestamp auto-detect priority list**: `["measured_on", "timestamp", "Date-Time", "datetime"]` — `measured_on` confirmed as universal across all accessible files.

**Alternatives considered**:
- Hardcode `measured_on` only: Would work for all known files but violates the spec clarification (auto-detect).
- Per-category column mapping: Unnecessary given consistent naming.

## R4: File Naming Convention Variation

**Decision**: Do not attempt to extract measurement category from filename for all sites. Instead, use the full S3 key basename (minus `.csv`) as the category identifier in the idempotency key.

**Rationale**: File naming conventions vary dramatically across the 4 target sites:

| Site | Pattern | Example |
| --- | --- | --- |
| 9068 | `{site_id}_{category}_data.csv` | `9068_ac_power_data.csv` |
| 2107 | `{site_id}_{category}_data.csv` | `2107_electrical_data.csv` |
| 9069 | `{site_id}_{subsystem}_{signal}.csv` (1000+ files) | `9069_Combiner DC Input_01.01.01_DC CURRENT STRING 01 (A).csv` |
| 7333 | `{site_id}_cmb_tag_list.csv_tag_group_{N}_{dates}_recorded.csv` | `7333_cmb_tag_list.csv_tag_group_1_20220101-20231231_recorded.csv` |

Attempting to parse a "category" from the filename would require per-site regex rules. Using the full filename as the category identifier is simpler, universally unique, and sufficient for idempotency key composition.

**Alternatives considered**:
- Regex extraction of category: Fragile, requires per-site rules.
- Use S3 `<Key>` as category: Too long for idempotency store RowKey; use basename instead.

## R5: File Tracking for Incremental Processing

**Decision**: Use Azure Table Storage to track which files have been processed. The dispatcher stores one row per file (keyed by S3 key hash) with status and metadata.

**Rationale**: The dispatcher must know which files have already been enqueued to avoid re-processing. A Table Storage entity per file (partition by site_id, row by filename hash) provides durable tracking with minimal cost. The `<LastModified>` and `<Size>` fields from S3 listing can be stored to detect file updates.

**Entity structure**:
- PartitionKey: site_id (string)
- RowKey: SHA-256 hash of S3 key (truncated to 64 chars)
- S3Key: full S3 object key
- Size: file size in bytes
- LastModified: S3 last-modified timestamp
- Status: "queued" | "processing" | "completed" | "failed"
- EnqueuedAt: timestamp when work item was dispatched
- CompletedAt: timestamp when worker finished (nullable)

**Alternatives considered**:
- In-memory tracking: Lost on function restart.
- Blob metadata: Higher latency, more complex.
- Separate database: Overkill for ~40 files.

## R6: Event Type Registration

**Decision**: Register `raw.pvdaq.historical.v1` in `topics.md` per constitution Principle VII.

**Rationale**: Historical data from the 2023-solar-data-prize is structurally different from the daily polling data (feature 001). A distinct event type allows downstream consumers to differentiate and route accordingly.

**Naming**: Follows the convention `raw.{vendor}.{data_category}.v{major}` from the constitution.

**Topic**: Same `raw-energy-events` topic — consumers can filter by event type.
