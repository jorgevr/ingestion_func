
"""Azure Functions entry point for PVDAQ ingestion.

Registers:
- Feature 001: Timer-triggered daily PVDAQ polling (pvdaq_ingestion)
- Feature 002: Timer-triggered historical dispatcher + queue-triggered worker

Data source: OEDI Data Lake (public S3 bucket).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from uuid import uuid4

import azure.functions as func

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
    ) as oedi_client, ServiceBusEmitter(
        fully_qualified_namespace=config.service_bus_fully_qualified_namespace,
    ) as emitter, IdempotencyStore(
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
    ) as tracker, ServiceBusEmitter(
        fully_qualified_namespace=config.service_bus_fully_qualified_namespace,
    ) as emitter:

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
                )
                work_item = {
                    "site_id": site_id,
                    "s3_key": file_info["key"],
                    "file_name": file_name,
                    "category": category,
                    "correlation_id": correlation_id,
                    "enqueued_at": datetime.now(timezone.utc).isoformat(),
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

    site_id: int = work_item["site_id"]
    s3_key: str = work_item["s3_key"]
    file_name: str = work_item["file_name"]
    category: str = work_item.get("category") or extract_category(file_name, site_id)
    correlation_id: str = work_item["correlation_id"]

    logger = create_logger(correlation_id, vendor="PVDAQ", function_name="historical_worker")
    start_time = time.monotonic()

    # Generate traceparent for this worker invocation
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]
    traceparent = f"00-{trace_id}-{span_id}-01"

    config = load_historical_config()
    stats = DatasetIngestionStats(source="PVDAQ-historical-worker", correlation_id=correlation_id)
    ingestion_id = str(uuid4())

    logger.info("Historical worker started — site=%d, file=%s", site_id, file_name)

    # Deterministic ADLS destination path (T012)
    adls_path = f"pvdaq/site_id={site_id}/category={category}/{file_name}"
    source_url = (
        f"{config.oedi_bucket_url.rstrip('/')}/{s3_key}"
    )

    async with FileTrackingStore(
        table_name=config.file_tracking_table_name,
        table_service_uri=config.table_storage_uri,
    ) as tracker, AdlsStore(
        account_url=config.adls_account_url,
        container_name=config.adls_container_name,
    ) as adls, ServiceBusEmitter(
        fully_qualified_namespace=config.service_bus_fully_qualified_namespace,
    ) as emitter:

        await tracker.mark_processing(site_id, s3_key)

        try:
            # Stream S3 → ADLS with incremental SHA-256 (one-pass, bounded memory)
            bytes_written, file_hash = await adls.stream_upload(
                source_url=source_url,
                file_path=adls_path,
            )
            stats.datasets_downloaded = 1
            stats.datasets_stored = 1

            ingestion_time = datetime.now(timezone.utc).isoformat()

            # Register metadata in tracking table (US2)
            await tracker.mark_completed(
                site_id=site_id,
                s3_key=s3_key,
                storage_path=adls_path,
                file_hash=file_hash,
                ingestion_id=ingestion_id,
                source_url=source_url,
                ingestion_time=ingestion_time,
            )

            # Emit dataset-available CloudEvent (US3)
            envelope = build_dataset_envelope(
                data={
                    "site_id": site_id,
                    "category": category,
                    "file_format": "csv",
                    "storage_path": adls_path,
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
                topic_name=config.service_bus_topic_name,
                envelope=envelope,
            )
            stats.datasets_emitted = 1

        except (AdlsUploadError, Exception) as exc:
            stats.datasets_failed = 1
            logger.error(
                "Historical worker failed — site=%d, file=%s: %s",
                site_id, file_name, exc, exc_info=True,
            )
            await tracker.mark_failed(site_id, s3_key)
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
