"""Shared per-record processing pipeline for PVDAQ ingestion functions.

Extracts the validate → dead-letter → idempotency → emit → mark-completed
sequence shared by feature 001 (daily) and feature 002 (historical).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from src.cloudevents_envelope import build_envelope
from src.config import Config, HistoricalConfig
from src.idempotency_store import IdempotencyResult, IdempotencyStore
from src.observability import InvocationStats
from src.schema_validator import validate_record
from src.service_bus_emitter import ServiceBusEmitter, build_dead_letter_message

logger = logging.getLogger(__name__)

# Type alias for configs that provide the fields we need
_ConfigLike = Config | HistoricalConfig


async def process_record(
    record: dict[str, Any],
    *,
    config: _ConfigLike,
    correlation_id: str,
    traceparent: str,
    emitter: ServiceBusEmitter,
    idem_store: IdempotencyStore,
    stats: InvocationStats,
    check_idempotency: Callable[..., Awaitable[IdempotencyResult]],
    mark_completed: Callable[..., Awaitable[None]],
    event_type: str = "raw.pvdaq.generation.v1",
    source: str = "/energy-ingestion-boundary/pvdaq",
) -> None:
    """Process a single validated record through the emission pipeline.

    Steps:
    1. Validate against pvdaq-v1.json schema
    2. Dead-letter if invalid
    3. Idempotency check (write-before-emit)
    4. Build CloudEvents envelope and emit
    5. Mark idempotency record as completed

    Args:
        record: Normalized PVDAQ record dict.
        config: App configuration (daily or historical).
        correlation_id: Invocation correlation ID.
        traceparent: W3C traceparent header.
        emitter: Service Bus emitter for events and dead-letters.
        idem_store: Idempotency store (unused directly — via callbacks).
        stats: Mutable invocation stats accumulator.
        check_idempotency: Async callable that performs the idempotency
            check and returns an ``IdempotencyResult``.
        mark_completed: Async callable to mark the record as completed
            after successful emission.
        event_type: CloudEvents ``type`` field.
        source: CloudEvents ``source`` field.
    """
    is_valid, errors = validate_record(record)

    if not is_valid:
        stats.number_invalid += 1
        dl_message = build_dead_letter_message(
            original_payload=record,
            error_details=errors,
            correlation_id=correlation_id,
            site_id=record.get("SiteID"),
            schema_version=config.schema_version_pvdaq,
        )
        await emitter.emit_dead_letter(
            queue_name=config.dead_letter_queue_name,
            message_body=dl_message,
        )
        return

    stats.number_valid += 1

    idem_result = await check_idempotency()

    if idem_result == IdempotencyResult.DUPLICATE:
        stats.number_duplicates += 1
        return

    envelope = build_envelope(
        record=record,
        config=config,
        correlation_id=correlation_id,
        traceparent=traceparent,
        event_type=event_type,
        source=source,
    )
    await emitter.emit_cloudevent(
        topic_name=config.service_bus_topic_name,
        envelope=envelope,
    )
    stats.number_emitted += 1

    await mark_completed()
