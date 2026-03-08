
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

from src.config import load_config, load_historical_config
from src.csv_normalizer import detect_timestamp_column, normalize_historical_record
from src.file_tracking_store import FileTrackingStore
from src.idempotency_store import IdempotencyStore
from src.observability import InvocationStats, create_logger, emit_invocation_metrics, emit_warning_metric
from src.oedi_data_lake import OediAccessError, OediDataLakeClient
from src.oedi_historical_client import OediHistoricalAccessError, OediHistoricalClient
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
    3. Enqueue a work-item message per unprocessed file
    """
    correlation_id = str(uuid4())
    logger = create_logger(correlation_id, vendor="PVDAQ", function_name="historical_dispatcher")
    start_time = time.monotonic()

    config = load_historical_config()

    total_discovered = 0
    total_enqueued = 0
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

            total_discovered += len(discovered)

            unprocessed = await tracker.get_unprocessed_files(site_id, discovered)
            if not unprocessed:
                logger.info("Site %d: all %d files already processed", site_id, len(discovered))
                continue

            for file_info in unprocessed:
                # Mark queued BEFORE sending to queue to avoid race condition
                # where the worker picks up the message before the entity exists.
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
                    "file_name": PurePosixPath(file_info["key"]).name,
                    "correlation_id": correlation_id,
                    "enqueued_at": datetime.now(timezone.utc).isoformat(),
                }
                await emitter.send_queue_message(
                    queue_name=config.pvdaq_historical_queue_name,
                    message_body=work_item,
                    subject="historical_work_item",
                )
                total_enqueued += 1

    duration_ms = (time.monotonic() - start_time) * 1000
    logger.info(
        "Historical dispatcher completed — discovered=%d, enqueued=%d, failed_sites=%s, duration_ms=%.0f",
        total_discovered,
        total_enqueued,
        failed_sites,
        duration_ms,
    )


@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="%PVDAQ_HISTORICAL_QUEUE_NAME%",
    connection="ServiceBusConnection",
)
async def historical_worker(msg: func.ServiceBusMessage) -> None:
    """Process a single historical CSV file from the work queue.

    Pipeline:
    1. Deserialize work item message
    2. Mark file as processing
    3. Stream CSV rows, normalize each record
    4. Validate against pvdaq-v1.json schema
    5. Emit valid records as CloudEvents, dead-letter invalid ones
    6. Mark file as completed/failed
    """
    raw_body = msg.get_body().decode("utf-8")
    work_item = json.loads(raw_body)

    site_id: int = work_item["site_id"]
    s3_key: str = work_item["s3_key"]
    file_name: str = work_item["file_name"]
    correlation_id: str = work_item["correlation_id"]

    logger = create_logger(correlation_id, vendor="PVDAQ", function_name="historical_worker")
    start_time = time.monotonic()

    # Generate traceparent for this worker invocation
    trace_id = uuid4().hex
    span_id = uuid4().hex[:16]
    traceparent = f"00-{trace_id}-{span_id}-01"

    config = load_historical_config()
    stats = InvocationStats(source="PVDAQ-historical", correlation_id=correlation_id)

    if config.mapping_version_pvdaq == "unknown":
        emit_warning_metric(
            metric_name="mapping_version_unknown",
            details={"mapping_version": config.mapping_version_pvdaq, "vendor": "PVDAQ"},
        )

    logger.info(
        "Historical worker started — site=%d, file=%s", site_id, file_name,
    )

    async with OediHistoricalClient(
        bucket_url=config.oedi_bucket_url,
        historical_prefix=config.oedi_historical_prefix,
    ) as oedi_client, FileTrackingStore(
        table_name=config.file_tracking_table_name,
        table_service_uri=config.table_storage_uri,
    ) as tracker, ServiceBusEmitter(
        fully_qualified_namespace=config.service_bus_fully_qualified_namespace,
    ) as emitter, IdempotencyStore(
        table_name=config.idempotency_table_name,
        table_service_uri=config.table_storage_uri,
    ) as idem_store:

        await tracker.mark_processing(site_id, s3_key)

        try:
            timestamp_column: str | None = None

            async for row in oedi_client.stream_csv_rows(s3_key):
                # Detect timestamp column from first row's headers
                if timestamp_column is None:
                    timestamp_column = detect_timestamp_column(list(row.keys()))

                record = normalize_historical_record(
                    row=row,
                    site_id=site_id,
                    file_name=file_name,
                    timestamp_column=timestamp_column,
                )
                if record is None:
                    continue

                stats.number_of_records_retrieved += 1

                measdatetime = record.get("measdatetime", "")
                await process_record(
                    record,
                    config=config,
                    correlation_id=correlation_id,
                    traceparent=traceparent,
                    emitter=emitter,
                    idem_store=idem_store,
                    stats=stats,
                    check_idempotency=lambda _md=measdatetime: idem_store.check_and_reserve_historical(
                        site_id=site_id,
                        file_name=file_name,
                        measdatetime=_md,
                        correlation_id=correlation_id,
                    ),
                    mark_completed=lambda _md=measdatetime: idem_store.mark_completed_for_historical(
                        site_id=site_id,
                        file_name=file_name,
                        measdatetime=_md,
                    ),
                    event_type="raw.pvdaq.historical.v1",
                    source="/energy-ingestion-boundary/pvdaq-historical",
                )

            await tracker.mark_completed(site_id, s3_key, stats.number_emitted)

        except Exception as exc:
            logger.error(
                "Historical worker failed — site=%d, file=%s: %s",
                site_id, file_name, exc, exc_info=True,
            )
            await tracker.mark_failed(site_id, s3_key)
            raise

    stats.duration_ms = (time.monotonic() - start_time) * 1000
    emit_invocation_metrics(stats)

    logger.info(
        "Historical worker completed — site=%d, file=%s, retrieved=%d, valid=%d, invalid=%d, emitted=%d, duration_ms=%.0f",
        site_id, file_name, stats.number_of_records_retrieved,
        stats.number_valid, stats.number_invalid, stats.number_emitted,
        stats.duration_ms,
    )
