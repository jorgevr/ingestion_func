"""End-to-end contract test: valid record -> CloudEvents envelope -> Service Bus."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import jsonschema
import pytest

from src.cloudevents_envelope import build_envelope
from src.config import Config
from src.service_bus_emitter import ServiceBusEmitter

_CE_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent.parent
    / "specs"
    / "001-pvdaq-ingestion"
    / "contracts"
    / "cloudevents-envelope.json"
)
_CE_SCHEMA: dict = json.loads(_CE_SCHEMA_PATH.read_text(encoding="utf-8"))

_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


class TestValidPayloadEndToEnd:
    """Build envelope, validate against schema, and verify Service Bus message."""

    def test_envelope_validates_against_contract(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        jsonschema.validate(instance=envelope, schema=_CE_SCHEMA)

    @pytest.mark.asyncio
    async def test_send_messages_called_with_correct_properties(
        self, sample_valid_record: dict, mock_config: Config, correlation_id: str
    ) -> None:
        envelope = build_envelope(
            record=sample_valid_record,
            config=mock_config,
            correlation_id=correlation_id,
            traceparent=_TRACEPARENT,
        )

        # Mock Service Bus
        mock_sender = AsyncMock()
        mock_sender.send_messages = AsyncMock()
        mock_sender.__aenter__ = AsyncMock(return_value=mock_sender)
        mock_sender.__aexit__ = AsyncMock(return_value=False)

        mock_client = MagicMock()
        mock_client.get_topic_sender = MagicMock(return_value=mock_sender)
        mock_client.close = AsyncMock()

        emitter = ServiceBusEmitter(
            fully_qualified_namespace=mock_config.service_bus_fully_qualified_namespace,
            client=mock_client,
        )

        await emitter.emit_cloudevent(
            topic_name=mock_config.service_bus_topic_name,
            envelope=envelope,
        )

        mock_sender.send_messages.assert_awaited_once()

        sent_message = mock_sender.send_messages.call_args[0][0]
        assert sent_message.content_type == "application/cloudevents+json"
        assert sent_message.subject == "raw.pvdaq.generation.v1"
