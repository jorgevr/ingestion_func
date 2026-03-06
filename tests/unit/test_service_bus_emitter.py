"""Unit tests for src.service_bus_emitter — Service Bus SDK sender.

Tests cover: emit_event method, emit_cloudevent method, emit_dead_letter method,
_get_client real-credential creation path, close/context manager lifecycle.

Mocked: Azure Service Bus SDK requires real Azure credentials and a
running Service Bus namespace. All tests use mock ServiceBusClient.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.service_bus_emitter import ServiceBusEmitter, build_dead_letter_message


def _mock_sender() -> AsyncMock:
    """Create a mock sender with async context manager support."""
    sender = AsyncMock()
    sender.send_messages = AsyncMock(return_value=None)
    sender.__aenter__ = AsyncMock(return_value=sender)
    sender.__aexit__ = AsyncMock(return_value=False)
    return sender


def _mock_client(topic_sender: AsyncMock | None = None, queue_sender: AsyncMock | None = None) -> MagicMock:
    """Create a mock ServiceBusClient."""
    client = MagicMock()
    client.get_topic_sender = MagicMock(return_value=topic_sender or _mock_sender())
    client.get_queue_sender = MagicMock(return_value=queue_sender or _mock_sender())
    client.close = AsyncMock(return_value=None)
    return client


class TestEmitEvent:
    """emit_event sends a JSON message to a Service Bus topic."""

    @pytest.mark.asyncio
    async def test_sends_message_to_topic(self) -> None:
        sender = _mock_sender()
        client = _mock_client(topic_sender=sender)
        emitter = ServiceBusEmitter(
            fully_qualified_namespace="test.servicebus.windows.net",
            client=client,
        )

        await emitter.emit_event(
            topic_name="test-topic",
            message_body={"key": "value"},
            content_type="application/json",
            subject="test-subject",
            application_properties={"custom": "prop"},
        )

        client.get_topic_sender.assert_called_once_with(topic_name="test-topic")
        sender.send_messages.assert_awaited_once()

        # Verify message content
        sent_message = sender.send_messages.call_args[0][0]
        raw_body = b"".join(sent_message.body)
        body = json.loads(raw_body)
        assert body == {"key": "value"}


class TestEmitCloudevent:
    """emit_cloudevent sends a CloudEvents envelope to a Service Bus topic."""

    @pytest.mark.asyncio
    async def test_sends_cloudevent_with_properties(self) -> None:
        sender = _mock_sender()
        client = _mock_client(topic_sender=sender)
        emitter = ServiceBusEmitter(
            fully_qualified_namespace="test.servicebus.windows.net",
            client=client,
        )

        envelope = {
            "specversion": "1.0",
            "type": "raw.pvdaq.generation.v1",
            "source_vendor": "PVDAQ",
            "schema_version": "v1",
            "data": {"SiteID": 2},
        }

        await emitter.emit_cloudevent(topic_name="test-topic", envelope=envelope)

        sender.send_messages.assert_awaited_once()
        sent_message = sender.send_messages.call_args[0][0]
        raw_body = b"".join(sent_message.body)
        body = json.loads(raw_body)
        assert body["type"] == "raw.pvdaq.generation.v1"


class TestGetClientCreatesCredential:
    """_get_client creates DefaultAzureCredential + ServiceBusClient when no client injected."""

    @pytest.mark.asyncio
    async def test_creates_client_with_credential(self) -> None:
        """Mocked: DefaultAzureCredential requires Azure environment."""
        mock_cred = MagicMock()
        mock_sb_client = MagicMock()

        with patch("src.service_bus_emitter.DefaultAzureCredential", return_value=mock_cred), \
             patch("src.service_bus_emitter.ServiceBusClient", return_value=mock_sb_client):
            emitter = ServiceBusEmitter(
                fully_qualified_namespace="test.servicebus.windows.net",
            )

            client = await emitter._get_client()

        assert client is mock_sb_client
        assert emitter._credential is mock_cred


class TestCloseLifecycle:
    """close() and async context manager properly clean up resources."""

    @pytest.mark.asyncio
    async def test_close_owned_client_and_credential(self) -> None:
        """When no client is injected, close() shuts down both client and credential."""
        emitter = ServiceBusEmitter(
            fully_qualified_namespace="test.servicebus.windows.net",
        )
        mock_client = AsyncMock()
        mock_client.close = AsyncMock(return_value=None)
        mock_cred = AsyncMock()
        mock_cred.close = AsyncMock(return_value=None)
        emitter._client = mock_client
        emitter._credential = mock_cred

        await emitter.close()

        mock_client.close.assert_called_once()
        mock_cred.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_injected_client_not_closed(self) -> None:
        """Injected client is not owned, so close() does not close it."""
        client = _mock_client()
        emitter = ServiceBusEmitter(
            fully_qualified_namespace="test.servicebus.windows.net",
            client=client,
        )

        await emitter.close()

        client.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """__aenter__ returns self, __aexit__ calls close."""
        client = _mock_client()

        async with ServiceBusEmitter(
            fully_qualified_namespace="test.servicebus.windows.net",
            client=client,
        ) as emitter:
            assert isinstance(emitter, ServiceBusEmitter)

        # close() was called via __aexit__


class TestBuildDeadLetterMessage:
    """build_dead_letter_message creates correct dead-letter payload."""

    def test_all_fields_present(self) -> None:
        msg = build_dead_letter_message(
            original_payload={"SiteID": 2, "measdatetime": "2026-01-15T12:00:00"},
            error_details=[{"message": "missing field", "path": "/SiteID", "validator": "required"}],
            correlation_id="test-corr",
            site_id=2,
            schema_version="v1",
        )

        assert msg["error_type"] == "validation_failure"
        assert msg["correlation_id"] == "test-corr"
        assert msg["site_id"] == 2
        assert msg["source_vendor"] == "PVDAQ"
        assert msg["schema_version"] == "v1"
        assert len(msg["error_details"]) == 1
        assert "timestamp" in msg
