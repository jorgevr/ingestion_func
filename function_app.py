
"""Azure Functions entry point for PVDAQ ingestion.

Registers:
- Feature 001: Timer-triggered daily PVDAQ polling (pvdaq_ingestion)
- Feature 002: Timer-triggered historical dispatcher + queue-triggered worker

Data source: OEDI Data Lake (public S3 bucket).
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import azure.functions as func
from jsonschema import Draft202012Validator, ValidationError

from src.adls_store import AdlsStore, AdlsUploadError
from src.cloudevents_envelope import build_dataset_envelope
from src.config import load_config, load_historical_config
from src.file_tracking_store import FileTrackingStore
from src.idempotency_store import IdempotencyStore
from src.observability import (
    DatasetIngestionStats,
    InvocationStats,
    create_logger,
    emit_dataset_metrics,
    emit_invocation_metrics,
    emit_warning_metric,
)
from src.oedi_data_lake import OediAccessError, OediDataLakeClient
from src.oedi_historical_client import (
    OediHistoricalAccessError,
    OediHistoricalClient,
    extract_category,
)
from src.record_pipeline import process_record
from src.service_bus_emitter import ServiceBusEmitter

app = func.FunctionApp()

# ---------------------------------------------------------------------------
# Feature 002: Module-level helpers
# ---------------------------------------------------------------------------

_WORK_ITEM_VALIDATOR: Draft202012Validator | None = None


def _adls_uri(account_url: str, container: str, path: str) -> str:
    """Build a full ADLS URI from account URL, container, and relative path.

    For Azurite (HTTP), returns ``http://host/account/container/path``.
    For production (HTTPS DFS endpoint), returns ``abfss://container@account.dfs.core.windows.net/path``.
    """
    from urllib.parse import urlparse
    parsed = urlparse(account_url)
    if parsed.scheme == "http":
        return f"{account_url.rstrip('/')}/{container}/{path}"
    # Production: https://account.dfs.core.windows.net → abfss://container@account.dfs.core.windows.net/path
    account_name = parsed.hostname.split(".")[0]
    return f"abfss://{container}@{account_name}.dfs.core.windows.net/{path}"


def _get_work_item_validator() -> Draft202012Validator:
    """Return a cached JSON Schema validator for historical work-item messages."""
    global _WORK_ITEM_VALIDATOR
    if _WORK_ITEM_VALIDATOR is None:
        schema_path = (
            Path(__file__).parent
            / "specs/002-pvdaq-historical-ingestion/contracts/work-item-message.json"
        )
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        _WORK_ITEM_VALIDATOR = Draft202012Validator(schema)
    return _WORK_ITEM_VALIDATOR


def _adls_path(site_id: int, category: str, ingestion_date: str, version: int) -> str:
    """Build the bronze-layer ADLS path for a versioned CSV ingestion.

    Path format (Constitution VIII + bronze folder convention):
    ``source=pvdaq/dataset={site_id}_{category}/ingestion_date={YYYY-MM-DD}/{dataset}_v{n}.csv``

    Args:
        site_id: PVDAQ site identifier.
        category: Measurement category (e.g. ``"ac_power"``).
        ingestion_date: UTC calendar date of ingestion in ``YYYY-MM-DD`` format.
        version: Ingestion version number (1 = first, increments on re-ingestion).
    """
    dataset = f"{site_id}_{category}"
    return (
        f"source=pvdaq"
        f"/dataset={dataset}"
        f"/ingestion_date={ingestion_date}"
        f"/{dataset}_v{version}.csv"
    )


def _metadata_path(site_id: int, category: str, ingestion_date: str) -> str:
    """Build the ADLS path for the ``metadata.json`` sidecar file.

    Written alongside the CSV at:
    ``source=pvdaq/dataset={site_id}_{category}/ingestion_date={YYYY-MM-DD}/metadata.json``
    """
    dataset = f"{site_id}_{category}"
    return (
        f"source=pvdaq"
        f"/dataset={dataset}"
        f"/ingestion_date={ingestion_date}"
        f"/metadata.json"
    )


def _storage_connection_string() -> str | None:
    """Return the storage connection string when running against the local emulator."""
    if os.environ.get("STORAGE_EMULATOR", "").lower() == "true":
        return "UseDevelopmentStorage=true"
    return None


def _make_emitter(config):
    """Return a ServiceBusEmitter for the current environment.

    Local development: ``ServiceBusConnection`` env var contains the emulator
    connection string (``UseDevelopmentEmulator=true``).
    Production: ``ServiceBusConnection__fullyQualifiedNamespace`` is used with
    ``DefaultAzureCredential`` (Managed Identity).
    """
    sb_conn_str = os.environ.get("ServiceBusConnection", "").strip()
    if sb_conn_str:
        return ServiceBusEmitter(connection_string=sb_conn_str)
    return ServiceBusEmitter(
        fully_qualified_namespace=config.service_bus_fully_qualified_namespace,
    )


def _date_range(lookback_hours: int) -> list[tuple[int, int, int]]:
    """Return a list of (year, month, day) tuples spanning the lookback window."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=lookback_hours)
    dates: list[tuple[int, int, int]] = []
    current = start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    while current <= end_of_today:
        dates.append((current.year, current.month, current.day))
        current += timedelta(days=1)
    return dates


@app.timer_trigger(
    schedule="%PVDAQ_CRON_SCHEDULE%",
    arg_name="timer",
    run_on_startup=False,
)
async def pvdaq_ingestion(timer: func.TimerRequest) -> None:
    """Ingest PVDAQ telemetry data on a CRON schedule.

    Pipeline per invocation:
    1. Generate correlation_id
    2. Load config
    3. Resolve site IDs (explicit list or discover from OEDI systems CSV)
    4. For each site (sequential), for each date in lookback window:
       a. Fetch daily CSV from OEDI Data Lake
       b. For each record:
          - Validate against pvdaq-v1.json
          - If valid: idempotency check -> enrich with CloudEvents envelope -> emit
          - If invalid: dead-letter with error metadata
    5. Log invocation summary
    """
    correlation_id = str(uuid4())
    logger = create_logger(correlation_id)
    start_time = time.monotonic()

    # Generate a traceparent for this invocation (no inbound trace context on timer triggers)
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]
    traceparent = f"00-{trace_id}-{span_id}-01"

    config = load_config()

    stats = InvocationStats(source="PVDAQ", correlation_id=correlation_id)

    # Emit warning if mapping_version is the sentinel value
    if config.mapping_version_pvdaq == "unknown":
        emit_warning_metric(
            metric_name="mapping_version_unknown",
            details={"mapping_version": config.mapping_version_pvdaq, "vendor": "PVDAQ"},
        )

    logger.info(
        "PVDAQ ingestion started — lookback_hours=%d",
        config.pvdaq_lookback_hours,
    )

    dates = _date_range(config.pvdaq_lookback_hours)
    failed_sites: list[int] = []

    async with OediDataLakeClient(
        bucket_url=config.oedi_bucket_url,
        systems_key=config.oedi_systems_key,
        data_prefix=config.oedi_data_prefix,
    ) as oedi_client, _make_emitter(config) as emitter, IdempotencyStore(
        table_name=config.idempotency_table_name,
        table_service_uri=config.table_storage_uri,
    ) as idem_store:

        # Resolve site IDs: use explicit list if configured, otherwise discover
        if config.pvdaq_site_ids:
            site_ids = config.pvdaq_site_ids
            logger.info("Using %d explicitly configured site IDs", len(site_ids))
        else:
            all_sites = await oedi_client.fetch_systems_list()
            site_ids = all_sites[: config.pvdaq_site_count]
            logger.info(
                "Discovered %d sites from OEDI, using first %d",
                len(all_sites),
                len(site_ids),
            )

        for site_id in site_ids:
            for year, month, day in dates:
                try:
                    records = await oedi_client.fetch_daily_site_data(
                        system_id=site_id,
                        year=year,
                        month=month,
                        day=day,
                    )
                except OediAccessError as exc:
                    logger.error(
                        "Failed to retrieve data for site %d on %04d-%02d-%02d: %s",
                        site_id,
                        year,
                        month,
                        day,
                        exc,
                    )
                    if site_id not in failed_sites:
                        failed_sites.append(site_id)
                    continue

                if not records:
                    continue

                stats.number_of_records_retrieved += len(records)

                for record in records:
                    try:
                        measdatetime = record.get("measdatetime", "")
                        record_site_id = record.get("SiteID", 0)
                        await process_record(
                            record,
                            config=config,
                            correlation_id=correlation_id,
                            traceparent=traceparent,
                            emitter=emitter,
                            idem_store=idem_store,
                            stats=stats,
                            check_idempotency=lambda: idem_store.check_and_reserve(
                                site_id=record_site_id,
                                measdatetime=measdatetime,
                                correlation_id=correlation_id,
                            ),
                            mark_completed=lambda: idem_store.mark_completed_for(
                                site_id=record_site_id,
                                measdatetime=measdatetime,
                            ),
                        )
                    except Exception as exc:
                        logger.error(
                            "Error processing record from site %s: %s",
                            record.get("SiteID", "unknown"),
                            exc,
                            exc_info=True,
                        )

    stats.duration_ms = (time.monotonic() - start_time) * 1000
    emit_invocation_metrics(stats)

    logger.info(
        "PVDAQ ingestion completed — retrieved=%d, valid=%d, invalid=%d, emitted=%d, duplicates=%d, failed_sites=%s",
        stats.number_of_records_retrieved,
        stats.number_valid,
        stats.number_invalid,
        stats.number_emitted,
        stats.number_duplicates,
        failed_sites,
    )


# ---------------------------------------------------------------------------
# Feature 002: Historical PVDAQ ingestion (fan-out dispatcher + worker)
# ---------------------------------------------------------------------------


@app.timer_trigger(
    schedule="%PVDAQ_HISTORICAL_CRON_SCHEDULE%",
    arg_name="timer",
    run_on_startup=False,
)
async def historical_dispatcher(timer: func.TimerRequest) -> None:
    """Discover historical CSV files on OEDI S3 and enqueue work items.

    For each configured site:
    1. List CSV files via S3 ListObjectsV2
    2. Filter to new/changed files via FileTrackingStore
    3. Enqueue a work-item message per unprocessed file (mark_queued before send)
    """
    correlation_id = str(uuid4())
    logger = create_logger(correlation_id, vendor="PVDAQ", function_name="historical_dispatcher")
    start_time = time.monotonic()

    config = load_historical_config()
    stats = DatasetIngestionStats(source="PVDAQ-historical-dispatcher", correlation_id=correlation_id)
    failed_sites: list[int] = []

    logger.info(
        "Historical dispatcher started — sites=%s",
        config.pvdaq_historical_site_ids,
    )

    async with OediHistoricalClient(
        bucket_url=config.oedi_bucket_url,
        historical_prefix=config.oedi_historical_prefix,
    ) as oedi_client, FileTrackingStore(
        table_name=config.file_tracking_table_name,
        table_service_uri=config.table_storage_uri,
        connection_string=_storage_connection_string(),
    ) as tracker, _make_emitter(config) as emitter:

        for site_id in config.pvdaq_historical_site_ids:
            try:
                discovered = await oedi_client.list_csv_files(site_id)
            except OediHistoricalAccessError as exc:
                logger.error(
                    "Failed to list files for site %d: %s", site_id, exc,
                )
                failed_sites.append(site_id)
                continue

            if not discovered:
                logger.warning("Site %d: no CSV files found", site_id)
                continue

            stats.datasets_discovered += len(discovered)

            unprocessed = await tracker.get_unprocessed_files(site_id, discovered)
            if not unprocessed:
                logger.info("Site %d: all %d files already processed", site_id, len(discovered))
                continue

            for file_info in unprocessed:
                file_name = PurePosixPath(file_info["key"]).name
                category = extract_category(file_name, site_id)

                # Mark queued BEFORE sending to queue — write-before-emit per Constitution VI
                await tracker.mark_queued(
                    site_id=site_id,
                    s3_key=file_info["key"],
                    file_size=file_info["size"],
                    last_modified=file_info.get("last_modified", ""),
                    correlation_id=correlation_id,
                    category=category,
                )
                work_item = {
                    "site_id": site_id,
                    "s3_key": file_info["key"],
                    "file_name": file_name,
                    "category": category,
                    "correlation_id": correlation_id,
                    "enqueued_at": datetime.now(timezone.utc).isoformat(),
                    "last_modified": file_info.get("last_modified", ""),
                }
                await emitter.send_queue_message(
                    queue_name=config.pvdaq_historical_queue_name,
                    message_body=work_item,
                    subject="historical_work_item",
                )

    stats.duration_ms = (time.monotonic() - start_time) * 1000
    emit_dataset_metrics(stats)

    logger.info(
        "Historical dispatcher completed — discovered=%d, failed_sites=%s, duration_ms=%.0f",
        stats.datasets_discovered,
        failed_sites,
        stats.duration_ms,
    )


@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="%PVDAQ_HISTORICAL_QUEUE_NAME%",
    connection="ServiceBusConnection",
)
async def historical_worker(msg: func.ServiceBusMessage) -> None:
    """Process a single historical CSV file: stream S3 → ADLS, register metadata, emit event.

    Pipeline:
    1. Deserialize work item message
    2. Mark file as processing
    3. Stream download from S3 + upload to ADLS Gen2 (one-pass, ~4 MiB memory)
    4. Update tracking entity with storage metadata
    5. Emit solar.pvdaq.dataset.available CloudEvent
    6. Mark file as completed
    """
    raw_body = msg.get_body().decode("utf-8")
    work_item = json.loads(raw_body)

    correlation_id: str = work_item.get("correlation_id", "unknown")
    logger = create_logger(correlation_id, vendor="PVDAQ", function_name="historical_worker")

    config = load_historical_config()

    # Validate work item schema — Constitution II: every inbound payload must be
    # validated against a versioned JSON Schema before processing begins.
    try:
        _get_work_item_validator().validate(work_item)
    except ValidationError as exc:
        logger.error("Work item schema validation failed: %s", exc.message)
        async with _make_emitter(config) as dl_emitter:
            await dl_emitter.emit_dead_letter(
                queue_name=config.dead_letter_queue_name,
                message_body={
                    "file_reference": raw_body,
                    "failure_reason": exc.message,
                    "error_type": "validation_failure",
                    "correlation_id": correlation_id,
                },
                subject="validation_failure",
            )
        return

    site_id: int = work_item["site_id"]
    s3_key: str = work_item["s3_key"]
    file_name: str = work_item["file_name"]
    category: str = work_item.get("category") or extract_category(file_name, site_id)

    start_time = time.monotonic()

    # Generate traceparent for this worker invocation
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]
    traceparent = f"00-{trace_id}-{span_id}-01"

    stats = DatasetIngestionStats(source="PVDAQ-historical-worker", correlation_id=correlation_id)
    ingestion_id = str(uuid4())

    logger.info("Historical worker started — site=%d, file=%s", site_id, file_name)

    source_url = f"{config.oedi_bucket_url.rstrip('/')}/{s3_key}"

    async with FileTrackingStore(
        table_name=config.file_tracking_table_name,
        table_service_uri=config.table_storage_uri,
        connection_string=_storage_connection_string(),
    ) as tracker, AdlsStore(
        account_url=config.adls_account_url,
        container_name=config.adls_container_name,
        connection_string=_storage_connection_string(),
    ) as adls, _make_emitter(config) as emitter:

        # Version resolution — determines append-only version before any writes
        existing_versions = await tracker.get_versions(site_id, s3_key)
        version = max(existing_versions, default=0) + 1

        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        adls_path = _adls_path(site_id, category, ingestion_date, version)
        meta_path = _metadata_path(site_id, category, ingestion_date)

        await tracker.mark_processing(site_id, s3_key, version)

        try:
            # Stream S3 → ADLS: one-pass, ~4 MiB memory, returns (bytes, sha256, newlines)
            bytes_written, file_hash, row_count = await adls.stream_upload(
                source_url=source_url,
                file_path=adls_path,
            )
            stats.datasets_downloaded = 1
            stats.datasets_stored = 1

            ingestion_time = datetime.now(timezone.utc).isoformat()
            elapsed = time.monotonic() - start_time

            # Build metadata.json conforming to contracts/metadata-file.json
            dataset_id = f"{site_id}_{category}"
            metadata_dict = {
                "dataset": {
                    "dataset_id": dataset_id,
                    "dataset_type": "time_series",
                    "version": version,
                    "schema_version": "unknown",
                    "tags": ["pvdaq", "solar"],
                },
                "source": {
                    "source": "pvdaq",
                    "source_type": "s3_public",
                    "endpoint": f"pvdaq/2023-solar-data-prize/{site_id}_OEDI/data/",
                    "provider": "NREL",
                    "region": "us-east-1",
                },
                "ingestion": {
                    "ingestion_time": ingestion_time,
                    "ingestion_id": ingestion_id,
                    "batch_id": correlation_id,
                    "pipeline": "energy-ingestion-boundary-v1",
                    "trigger_type": "rerun" if version > 1 else "scheduled",
                    "retry_count": 0,
                    "source_file_name": file_name,
                    "file_size_bytes": bytes_written,
                    "checksum": file_hash,
                    "ingestion_latency_seconds": round(elapsed, 1),
                    "status": "success",
                },
                "event_time": {
                    "event_time_start": None,
                    "event_time_end": None,
                    "expected_frequency_seconds": 300,
                    "expected_records": None,
                },
                "data_profile": {
                    "row_count": row_count,
                    "null_percentage": None,
                    "duplicate_rows": None,
                    "min_timestamp": None,
                    "max_timestamp": None,
                    "schema_detected": None,
                    "corrupted_rows": None,
                },
                "quality_hint": {
                    "basic_quality_score": None,
                    "schema_valid": None,
                    "time_continuity_suspected_gap": None,
                    "notes": [],
                },
                "lineage": {
                    "parent_dataset_version": version - 1 if version > 1 else None,
                    "rerun_of": f"{dataset_id}_v{version - 1}" if version > 1 else None,
                    "related_incident_id": None,
                },
            }
            await adls.write_json(meta_path, metadata_dict)

            # Register metadata in tracking table (US2)
            await tracker.mark_completed(
                site_id=site_id,
                s3_key=s3_key,
                version=version,
                category=category,
                storage_path=adls_path,
                file_hash=file_hash,
                ingestion_id=ingestion_id,
                source_url=source_url,
                ingestion_time=ingestion_time,
                row_count=row_count,
                metadata_path=meta_path,
            )

            # Emit dataset-available CloudEvent (US3)
            envelope = build_dataset_envelope(
                data={
                    "site_id": site_id,
                    "category": category,
                    "file_format": "csv",
                    "storage_path": _adls_uri(config.adls_account_url, config.adls_container_name, adls_path),
                    "version": version,
                    "ingestion_id": ingestion_id,
                    "source_url": source_url,
                    "file_size": bytes_written,
                    "file_hash": file_hash,
                },
                config=config,
                ingestion_id=ingestion_id,
                traceparent=traceparent,
                ingestion_timestamp=ingestion_time,
            )
            await emitter.emit_cloudevent(
                topic_name=config.service_bus_queue_name,
                envelope=envelope,
            )
            stats.datasets_emitted = 1

        except (AdlsUploadError, Exception) as exc:
            stats.datasets_failed = 1
            logger.error(
                "Historical worker failed — site=%d, file=%s: %s",
                site_id, file_name, exc, exc_info=True,
            )
            await tracker.mark_failed(site_id, s3_key, version)
            await emitter.emit_dead_letter(
                queue_name=config.dead_letter_queue_name,
                message_body={
                    "file_reference": s3_key,
                    "failure_reason": str(exc),
                    "correlation_id": correlation_id,
                },
                subject="dataset_failure",
            )
            raise

    stats.duration_ms = (time.monotonic() - start_time) * 1000
    emit_dataset_metrics(stats)

    logger.info(
        "Historical worker completed — site=%d, file=%s, bytes=%d, stored=%d, emitted=%d, duration_ms=%.0f",
        site_id, file_name, bytes_written,
        stats.datasets_stored, stats.datasets_emitted,
        stats.duration_ms,
    )
