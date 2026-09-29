"""Azure Functions entry point for PVDAQ ingestion.

Registers:
- Feature 001: Timer-triggered daily PVDAQ polling (pvdaq_ingestion)
- Feature 002: Timer-triggered historical dispatcher + queue-triggered worker

Data source: OEDI Data Lake (public S3 bucket).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

import azure.functions as func
import httpx
from azure.core.exceptions import (
    HttpResponseError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.servicebus.exceptions import (
    MessageNotFoundError,
    MessageSizeExceededError,
    MessagingEntityAlreadyExistsError,
    MessagingEntityDisabledError,
    MessagingEntityNotFoundError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    ServiceBusError,
)
from jsonschema import Draft202012Validator, ValidationError

from src.adls_store import AdlsStore
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
# Fail fast: validate configuration AND compile schemas once at import time
# (worker startup), not lazily on the first invocation. A misconfigured
# environment or a missing/invalid schema then keeps this worker process
# from finishing initialization at all, instead of registering triggers
# that only discover the problem when a message finally arrives (a schema
# load failure inside the trigger is exactly what caused ~8,000 immediate
# redeliveries: an uncaught FileNotFoundError on every message). Note: this
# has only been verified to stop the Functions host from registering any
# triggers (confirmed via /admin/functions returning an empty list) — the
# docker-compose healthcheck only probes /admin/host/status, which stays
# "Running" regardless, so it does NOT currently surface this as an
# unhealthy container. Both config loaders' return values are discarded
# here; each trigger still calls its own loader per invocation to get the
# live Config.
#
# Every schema path is resolved relative to this package (Path(__file__))
# under schemas/ — the one directory the Dockerfile actually copies into the
# image. Never specs/ or docs/: those exist only in local dev, so a path
# built from either looks fine on a laptop and 404s in the container.
# tests/unit/test_schema_packaging.py guards this at the path level.
# ---------------------------------------------------------------------------
load_config()
load_historical_config()

_WORK_ITEM_SCHEMA_PATH = Path(__file__).parent / "schemas" / "work-item.v1.json"
_WORK_ITEM_SCHEMA: dict = json.loads(_WORK_ITEM_SCHEMA_PATH.read_text(encoding="utf-8"))
_WORK_ITEM_VALIDATOR = Draft202012Validator(_WORK_ITEM_SCHEMA)

# ---------------------------------------------------------------------------
# Feature 002: Module-level helpers
# ---------------------------------------------------------------------------


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
        f"source=pvdaq/dataset={dataset}/ingestion_date={ingestion_date}/metadata.json"
    )


def _storage_connection_string(
    logger: logging.LoggerAdapter | None = None,
) -> str | None:
    """Return the local storage connection string when configured, else ``None``.

    ``DATA_STORAGE_CONNECTION`` (e.g. pointing at Azurite) selects the
    connection-string branch of the Blob/Table clients; when unset, callers
    fall back to their account-URL + ``DefaultAzureCredential`` branch (cloud).
    Named to match ``DATA_STORAGE_ACCOUNT_URL`` / ``BRONZE_CONTAINER`` — the
    cross-service storage config names this repo has adopted per the
    Contract Owner's naming request, pending a matching write-up in
    docs/contracts.md. When *logger* is given, logs which mode was selected
    for this invocation.
    """
    conn_str = os.environ.get("DATA_STORAGE_CONNECTION", "").strip() or None
    if logger is not None:
        mode = "connection_string" if conn_str else "default_azure_credential"
        logger.info("Storage client mode selected: %s", mode)
    return conn_str


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


async def _dead_letter(
    config,
    logger: logging.LoggerAdapter,
    correlation_id: str,
    reason: str,
    detail: dict,
) -> None:
    """Single choke point for every dead-letter ``historical_worker`` sends.

    The body stays this repo's current app-level queue shape — a flat dict
    with ``correlation_id``, ``error_type`` (== *reason*), and whatever
    *detail* supplies (``file_reference``, ``failure_reason``). R2.6 (ADR
    0001) replaces this body with a quarantine record; keeping every
    dead-letter site routed through here means that swap only touches one
    place, not three.

    If the send itself fails, this logs an ERROR (naming *reason* and
    *correlation_id*) and re-raises — the caller then also raises (or lets
    this propagate), so the *original* message is never completed and is
    redelivered instead, bounded by the queue's ``maxDeliveryCount``. Losing
    the dead-letter send must never look like "handled".
    """
    try:
        async with _make_emitter(config) as emitter:
            await emitter.emit_dead_letter(
                queue_name=config.dead_letter_queue_name,
                message_body={
                    "correlation_id": correlation_id,
                    "error_type": reason,
                    **detail,
                },
                subject=reason,
            )
    except Exception:
        logger.error(
            "Failed to send dead-letter message — reason=%s correlation_id=%s",
            reason,
            correlation_id,
            exc_info=True,
        )
        raise


# ServiceBusError itself defaults to transient (below) — this is the
# curated exception list: subclasses where a retry cannot help (auth,
# entity gone/disabled/already-exists, message permanently too big, a
# message that genuinely does not exist). Anything NOT listed here —
# including a future SDK subclass this code has never heard of — stays
# transient by falling through to the base-class check, which is the safe
# default for Service Bus specifically (most failures here really are
# connection/timeout/throttle). Quota/lock-lost errors are deliberately
# *not* listed: they're transient (a retry, possibly after backoff, can
# succeed once quota frees up or a new lock is acquired) — the base default
# already covers them. tests/unit/test_servicebus_error_classification.py
# recursively enumerates every ServiceBusError subclass in the installed
# SDK and asserts each has an explicit expected classification here,
# documenting each one instead of relying on silent fallthrough.
_DETERMINISTIC_SERVICE_BUS_ERRORS = (
    MessagingEntityNotFoundError,
    MessagingEntityDisabledError,
    MessagingEntityAlreadyExistsError,
    ServiceBusAuthenticationError,
    ServiceBusAuthorizationError,
    MessageSizeExceededError,
    MessageNotFoundError,
)


def _is_transient_single(exc: BaseException) -> bool:
    """Classify one exception (not its cause chain) as transient."""
    if isinstance(exc, ServiceRequestError | ServiceResponseError):
        return True
    if isinstance(exc, HttpResponseError):
        status = exc.status_code
        return status is not None and (status >= 500 or status in (408, 429))
    if isinstance(exc, ServiceBusError):
        return not isinstance(exc, _DETERMINISTIC_SERVICE_BUS_ERRORS)
    if isinstance(exc, httpx.TimeoutException | httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        return status >= 500 or status in (408, 429)
    return False


def _is_transient(exc: BaseException) -> bool:
    """True only for network, throttling, and 5xx/408/429 failures — worth
    retrying. Classifies the exception itself and walks its ``__cause__``
    chain (e.g. ``AdlsUploadError`` wraps the real SDK exception via
    ``raise ... from exc``), so this works directly on whatever any of the
    four guarded call sites (stream_upload, write_json, emit_cloudevent,
    mark_completed) actually raises — no special-casing by wrapper type.

    Everything else (a missing source file, a malformed work item, an
    unclassified error, a bug) is deterministic and must dead-letter on the
    first attempt rather than redeliver forever (AGENTS.md §6 "Definition
    of done"): retrying a 404 against the same URL, or a genuine defect,
    can never succeed, and doing so anyway is exactly what turned one bad
    message into ~8,000 immediate redeliveries.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _is_transient_single(current):
            return True
        current = current.__cause__
    return False


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
            details={
                "mapping_version": config.mapping_version_pvdaq,
                "vendor": "PVDAQ",
            },
        )

    logger.info(
        "PVDAQ ingestion started — lookback_hours=%d",
        config.pvdaq_lookback_hours,
    )

    dates = _date_range(config.pvdaq_lookback_hours)
    failed_sites: list[int] = []

    async with (
        OediDataLakeClient(
            bucket_url=config.oedi_bucket_url,
            systems_key=config.oedi_systems_key,
            data_prefix=config.oedi_data_prefix,
        ) as oedi_client,
        _make_emitter(config) as emitter,
        IdempotencyStore(
            table_name=config.idempotency_table_name,
            table_service_uri=config.table_storage_uri,
            connection_string=_storage_connection_string(logger),
        ) as idem_store,
    ):
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
                            check_idempotency=lambda site_id=record_site_id, measdatetime=measdatetime: (
                                idem_store.check_and_reserve(
                                    site_id=site_id,
                                    measdatetime=measdatetime,
                                    correlation_id=correlation_id,
                                )
                            ),
                            mark_completed=lambda site_id=record_site_id, measdatetime=measdatetime: (
                                idem_store.mark_completed_for(
                                    site_id=site_id,
                                    measdatetime=measdatetime,
                                )
                            ),
                        )
                    except Exception:
                        logger.exception(
                            "Error processing record from site %s",
                            record.get("SiteID", "unknown"),
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
    logger = create_logger(
        correlation_id, vendor="PVDAQ", function_name="historical_dispatcher"
    )
    start_time = time.monotonic()

    config = load_historical_config()
    stats = DatasetIngestionStats(
        source="PVDAQ-historical-dispatcher", correlation_id=correlation_id
    )
    failed_sites: list[int] = []

    logger.info(
        "Historical dispatcher started — sites=%s",
        config.pvdaq_historical_site_ids,
    )

    async with (
        OediHistoricalClient(
            bucket_url=config.oedi_bucket_url,
            historical_prefix=config.oedi_historical_prefix,
        ) as oedi_client,
        FileTrackingStore(
            table_name=config.file_tracking_table_name,
            table_service_uri=config.table_storage_uri,
            connection_string=_storage_connection_string(logger),
        ) as tracker,
        _make_emitter(config) as emitter,
    ):
        for site_id in config.pvdaq_historical_site_ids:
            try:
                discovered = await oedi_client.list_csv_files(site_id)
            except OediHistoricalAccessError as exc:
                logger.error(
                    "Failed to list files for site %d: %s",
                    site_id,
                    exc,
                )
                failed_sites.append(site_id)
                continue

            if not discovered:
                logger.warning("Site %d: no CSV files found", site_id)
                continue

            stats.datasets_discovered += len(discovered)

            unprocessed = await tracker.get_unprocessed_files(site_id, discovered)
            if not unprocessed:
                logger.info(
                    "Site %d: all %d files already processed", site_id, len(discovered)
                )
                continue

            for file_info in unprocessed:
                last_modified = file_info.get("last_modified")
                if not last_modified:
                    # No default: an empty last_modified breaks get_unprocessed_files'
                    # change-detection (it would always look "changed") and now
                    # fails work-item schema validation (minLength: 1) anyway —
                    # skip and log rather than enqueue a work item we know is bad.
                    logger.warning(
                        "Site %d: skipping %s — S3 listing has no last_modified",
                        site_id,
                        file_info["key"],
                    )
                    continue

                file_name = PurePosixPath(file_info["key"]).name
                category = extract_category(file_name, site_id)

                # Mark queued BEFORE sending to queue — write-before-emit per Constitution VI
                await tracker.mark_queued(
                    site_id=site_id,
                    s3_key=file_info["key"],
                    file_size=file_info["size"],
                    last_modified=last_modified,
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
                    "last_modified": last_modified,
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
    """Process a single historical CSV file: stream S3 → ADLS, emit event, mark complete.

    Pipeline (all one classified try region, from config load through
    mark_completed):
    1. Load config
    2. Parse the message body (deterministic on failure — dead-letter, no redeliver)
    3. Validate work item schema (deterministic on failure — dead-letter, no redeliver)
    4. Construct the stores, resolve the version, mark file as processing
    5. Stream download from S3 + upload to ADLS Gen2 (one-pass, ~4 MiB memory)
    6. Write the metadata.json sidecar
    7. Emit the solar.pvdaq.dataset.available CloudEvent
    8. Mark file as completed — LAST, because "completed" must mean "event
       published". A transient failure at any of steps 5-7 leaves the
       tracker showing no completed version, so a redelivery of the same
       message resolves the *same* version again and re-emits — and the
       CloudEvent id is deterministic (s3_key+version), so that re-emit is
       a detectable duplicate for the consumer, not a new event.

    Body-parsing and schema-validation failures are handled by their own
    inner try/except (always deterministic, never classified — a malformed
    body or an invalid work item is never made valid by retrying) and
    return directly. Every other failure — from config load through
    mark_completed — is caught by the single outer `except`, which is the
    one place classification (`_is_transient`) happens: config-load failure
    has no config to dead-letter with, so it always re-raises; every other
    failure dead-letters and then raises only if transient.

    Every exit path runs through the same `finally` so dataset metrics are
    always emitted, and stats.datasets_failed reflects every failure — not
    only the ones that happen to fall through the bottom of the function.
    """
    raw_body_bytes = msg.get_body()

    correlation_id = "unknown"
    logger = create_logger(
        correlation_id, vendor="PVDAQ", function_name="historical_worker"
    )
    stats = DatasetIngestionStats(
        source="PVDAQ-historical-worker", correlation_id=correlation_id
    )
    start_time = time.monotonic()
    config = None
    site_id: int | None = None
    s3_key: str | None = None
    version: int | None = None
    file_name = "unknown"
    bytes_written = 0

    try:
        config = load_historical_config()

        # --- Parse the body: decode, JSON-load, require a JSON object.
        # A malformed body will never parse no matter how many times Service
        # Bus redelivers the identical bytes — deterministic, dead-letter
        # once and complete the message.
        try:
            raw_body = raw_body_bytes.decode("utf-8")
            work_item = json.loads(raw_body)
            if not isinstance(work_item, dict):
                raise TypeError(
                    "work item body must be a JSON object, got "
                    f"{type(work_item).__name__}"
                )
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
            stats.datasets_failed = 1
            logger.error("Work item body could not be parsed: %s", exc)
            preview = raw_body_bytes.decode("utf-8", errors="replace")[:500]
            await _dead_letter(
                config,
                logger,
                correlation_id,
                reason="malformed_body",
                detail={"file_reference": preview, "failure_reason": str(exc)},
            )
            return

        correlation_id = work_item.get("correlation_id", "unknown")
        logger = create_logger(
            correlation_id, vendor="PVDAQ", function_name="historical_worker"
        )
        stats.correlation_id = correlation_id

        # Validate work item schema — Constitution II: every inbound payload must be
        # validated against a versioned JSON Schema before processing begins.
        try:
            _WORK_ITEM_VALIDATOR.validate(work_item)
        except ValidationError as exc:
            stats.datasets_failed = 1
            logger.error("Work item schema validation failed: %s", exc.message)
            await _dead_letter(
                config,
                logger,
                correlation_id,
                reason="validation_failure",
                detail={"file_reference": raw_body, "failure_reason": exc.message},
            )
            return

        site_id = work_item["site_id"]
        s3_key = work_item["s3_key"]
        file_name = work_item["file_name"]
        category: str = work_item.get("category") or extract_category(
            file_name, site_id
        )

        # Generate traceparent for this worker invocation
        trace_id = uuid4().hex
        span_id = uuid4().hex[:16]
        traceparent = f"00-{trace_id}-{span_id}-01"
        ingestion_id = str(uuid4())

        logger.info("Historical worker started — site=%d, file=%s", site_id, file_name)

        source_url = f"{config.oedi_bucket_url.rstrip('/')}/{s3_key}"
        storage_conn_str = _storage_connection_string(logger)

        async with (
            FileTrackingStore(
                table_name=config.file_tracking_table_name,
                table_service_uri=config.table_storage_uri,
                connection_string=storage_conn_str,
            ) as tracker,
            AdlsStore(
                account_url=config.data_storage_account_url,
                container_name=config.bronze_container,
                connection_string=storage_conn_str,
            ) as adls,
            _make_emitter(config) as emitter,
        ):
            # Version resolution — determines append-only version before any writes.
            # get_versions only counts *completed* versions, and mark_completed
            # now runs last (after emit_cloudevent), so a redelivery following a
            # transient failure resolves this same version again — see the
            # docstring above.
            existing_versions = await tracker.get_versions(site_id, s3_key)
            version = max(existing_versions, default=0) + 1

            ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            adls_path = _adls_path(site_id, category, ingestion_date, version)
            meta_path = _metadata_path(site_id, category, ingestion_date)

            await tracker.mark_processing(site_id, s3_key, version)

            # Stream S3 → ADLS: one-pass, ~4 MiB memory, returns (bytes, sha256, newlines)
            bytes_written, file_hash, row_count = await adls.stream_upload(
                source_url=source_url,
                file_path=adls_path,
                s3_key=s3_key,
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

            # Emit dataset-available CloudEvent (US3) — before mark_completed;
            # "completed" must mean "event published".
            envelope = build_dataset_envelope(
                data={
                    "site_id": site_id,
                    "category": category,
                    "file_format": "csv",
                    "storage_path": await adls.blob_url(adls_path),
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
                s3_key=s3_key,
            )
            await emitter.emit_cloudevent(
                topic_name=config.service_bus_queue_name,
                envelope=envelope,
            )
            stats.datasets_emitted = 1

            # Register metadata in tracking table — LAST: this is what
            # get_versions() reads to decide "already completed", so it
            # must not be set until the event it gates has been sent.
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

        logger.info(
            "Historical worker completed — site=%d, file=%s, bytes=%d, stored=%d, emitted=%d",
            site_id,
            file_name,
            bytes_written,
            stats.datasets_stored,
            stats.datasets_emitted,
        )

    except Exception as exc:
        stats.datasets_failed = 1

        if config is None:
            # load_historical_config() itself failed: there is no config to
            # dead-letter with (and no way to construct one), so this
            # cannot be classified or dead-lettered here — it must raise
            # and let the host's own redelivery policy handle it.
            logger.exception("Historical worker failed before config could be loaded")
            raise

        logger.exception(
            "Historical worker failed — site=%s, file=%s",
            site_id,
            file_name,
        )

        if site_id is not None and s3_key is not None and version is not None:
            # A fresh store, not the enclosing `tracker` — if the failure
            # happened inside the `async with` block, `tracker` may already
            # be closed by the time this except runs.
            try:
                async with FileTrackingStore(
                    table_name=config.file_tracking_table_name,
                    table_service_uri=config.table_storage_uri,
                    connection_string=_storage_connection_string(),
                ) as fresh_tracker:
                    await fresh_tracker.mark_failed(site_id, s3_key, version)
            except Exception:
                logger.error(
                    "Failed to mark file as failed after worker error",
                    exc_info=True,
                )

        await _dead_letter(
            config,
            logger,
            correlation_id,
            reason="dataset_failure",
            detail={
                "file_reference": s3_key or "unknown",
                "failure_reason": str(exc),
            },
        )
        # Deterministic failures (a missing source file, a bug,
        # anything not explicitly recognised as transient) complete
        # the message here instead of raising — see _is_transient.
        if _is_transient(exc):
            raise
        return
    finally:
        stats.duration_ms = (time.monotonic() - start_time) * 1000
        emit_dataset_metrics(stats)
