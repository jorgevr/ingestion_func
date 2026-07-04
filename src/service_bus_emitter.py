"""Service Bus SDK sender for PVDAQ ingestion function.

Sends validated CloudEvents to a Service Bus topic, and routes validation
failures to a dead-letter queue.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from azure.identity.aio import DefaultAzureCredential
from azure.servicebus.aio import ServiceBusClient, ServiceBusSender
from azure.servicebus import ServiceBusMessage


def build_dead_letter_message(
    original_payload: dict,
    error_details: list[dict],
    correlation_id: str,
    site_id: int | None,
    schema_version: str,
) -> dict:
    """Construct a dead-letter message matching the dead-letter-message.json contract.

    Args:
        original_payload: The raw PVDAQ record that failed validation.
        error_details: List of validation error dicts (message, path, validator).
        correlation_id: Invocation correlation ID for tracing.
        site_id: PVDAQ site ID if available, otherwise ``None``.
        schema_version: Schema version used for validation (e.g. ``"v1"``).

    Returns:
        A dict conforming to ``specs/001-pvdaq-ingestion/contracts/dead-letter-message.json``.
    """
    return {
        "original_payload": original_payload,
        "error_type": "validation_failure",
        "error_details": error_details,
        "correlation_id": correlation_id,
        "site_id": site_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_vendor": "PVDAQ",
        "schema_version": schema_version,
    }


class ServiceBusEmitter:
    """Async sender that emits messages to Service Bus topics and queues.

    Args:
        fully_qualified_namespace: The ``<name>.servicebus.windows.net`` namespace.
            Used with ``DefaultAzureCredential`` when ``connection_string`` is not set.
        connection_string: Full Service Bus connection string (e.g. for the local emulator).
            Takes precedence over ``fully_qualified_namespace`` when provided.
        client: Optional pre-built ``ServiceBusClient`` for dependency injection
            and testability.  When *None*, a client is created automatically.
    """

    def __init__(
        self,
        fully_qualified_namespace: str | None = None,
        client: ServiceBusClient | None = None,
        connection_string: str | None = None,
    ) -> None:
        self._namespace = fully_qualified_namespace
        self._connection_string = connection_string
        self._client = client
        self._credential: DefaultAzureCredential | None = None
        self._owns_client = client is None

    async def _get_client(self) -> ServiceBusClient:
        if self._client is None:
            if self._connection_string:
                self._client = ServiceBusClient.from_connection_string(self._connection_string)
            else:
                self._credential = DefaultAzureCredential()
                self._client = ServiceBusClient(
                    fully_qualified_namespace=self._namespace,
                    credential=self._credential,
                )
        return self._client

    async def send_queue_message(
        self,
        queue_name: str,
        message_body: dict[str, Any],
        content_type: str = "application/json",
        subject: str | None = None,
    ) -> None:
        """Send a single message to a Service Bus queue.

        Args:
            queue_name: Target queue name.
            message_body: Message payload (will be JSON-serialised).
            content_type: MIME content type (default ``application/json``).
            subject: Optional message subject / label.
        """
        client = await self._get_client()
        sender: ServiceBusSender
        async with client.get_queue_sender(queue_name=queue_name) as sender:
            message = ServiceBusMessage(
                body=json.dumps(message_body),
                content_type=content_type,
                subject=subject,
            )
            await sender.send_messages(message)

    async def emit_dead_letter(
        self,
        queue_name: str,
        message_body: dict[str, Any],
        content_type: str = "application/json",
        subject: str = "validation_failure",
    ) -> None:
        """Send a message to the dead-letter queue.

        Args:
            queue_name: Target dead-letter queue name.
            message_body: Message payload (will be JSON-serialised).
            content_type: MIME content type (default ``application/json``).
            subject: Message subject (default ``validation_failure``).
        """
        await self.send_queue_message(queue_name, message_body, content_type, subject)

    async def emit_cloudevent(
        self,
        topic_name: str,
        envelope: dict,
    ) -> None:
        """Send a CloudEvents envelope to a Service Bus queue.

        Args:
            topic_name: Target queue name (parameter kept as ``topic_name`` for
                interface compatibility; always routes to a queue — Basic tier only).
            envelope: A CloudEvents envelope dict (will be JSON-serialised).
        """
        client = await self._get_client()
        sender: ServiceBusSender
        async with client.get_queue_sender(queue_name=topic_name) as sender:
            message = ServiceBusMessage(
                body=json.dumps(envelope),
                content_type="application/cloudevents+json",
                subject=envelope["type"],
                application_properties={
                    "source_vendor": envelope["source_vendor"],
                    "schema_version": envelope["schema_version"],
                },
            )
            await sender.send_messages(message)

    async def close(self) -> None:
        """Close the underlying client and credential if owned by this emitter."""
        if self._client is not None and self._owns_client:
            await self._client.close()
        if self._credential is not None:
            await self._credential.close()

    async def __aenter__(self) -> ServiceBusEmitter:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
