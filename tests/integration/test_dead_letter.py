"""Integration test: invalid record flows through validation to dead-letter queue."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import Config
from src.schema_validator import validate_record
from src.service_bus_emitter import ServiceBusEmitter, build_dead_letter_message


class TestDeadLetterIntegration:
    """Submit an invalid record through the validation + DLQ pipeline."""

    @pytest.mark.asyncio
    async def test_invalid_record_emitted_to_dlq(
        self,
        sample_invalid_record: dict,
        mock_config: Config,
        correlation_id: str,
    ) -> None:
        # Step 1: validate the record
        is_valid, error_details = validate_record(sample_invalid_record)
        assert is_valid is False

        # Step 2: build the dead-letter message
        dlq_message = build_dead_letter_message(
            original_payload=sample_invalid_record,
            error_details=error_details,
            correlation_id=correlation_id,
            site_id=sample_invalid_record.get("SiteID"),
            schema_version=mock_config.schema_version_pvdaq,
        )

        # Step 3: set up mocked Service Bus
        mock_sender = AsyncMock()
        mock_sender.send_messages = AsyncMock()
        mock_sender.__aenter__ = AsyncMock(return_value=mock_sender)
        mock_sender.__aexit__ = AsyncMock(return_value=False)

        mock_client = MagicMock()
        mock_client.get_queue_sender = MagicMock(return_value=mock_sender)
        mock_client.close = AsyncMock()

        emitter = ServiceBusEmitter(
            fully_qualified_namespace=mock_config.service_bus_fully_qualified_namespace,
            client=mock_client,
        )

        # Step 4: emit to DLQ
        await emitter.emit_dead_letter(
            queue_name=mock_config.dead_letter_queue_name,
            message_body=dlq_message,
        )

        # Step 5: assertions
        mock_client.get_queue_sender.assert_called_once_with(
            queue_name=mock_config.dead_letter_queue_name,
        )
        mock_sender.send_messages.assert_awaited_once()

        # Verify the message body contains the expected structure
        sent_message = mock_sender.send_messages.call_args[0][0]
        # ServiceBusMessage.body is a generator; collect bytes and decode
        raw_body = b"".join(sent_message.body)
        body = json.loads(raw_body)
        assert body["error_type"] == "validation_failure"
        assert body["original_payload"] == sample_invalid_record
        assert body["correlation_id"] == correlation_id
        assert len(body["error_details"]) >= 1
