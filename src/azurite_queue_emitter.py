"""Azure Storage Queue emitter for local Azurite emulation.

Provides the same interface as ServiceBusEmitter, routing messages to
Azure Storage Queues instead of Service Bus. Used when STORAGE_EMULATOR=true.
"""

from __future__ import annotations

import json
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.storage.queue.aio import QueueServiceClient


class AzuriteQueueEmitter:
    """Sends messages to Azure Storage Queues (Azurite or production storage).

    Implements the same interface as ServiceBusEmitter for transparent local
    emulation switching via ``STORAGE_EMULATOR=true``.

    Args:
        connection_string: Azure Storage connection string.
            For Azurite use ``UseDevelopmentStorage=true``.
    """

    def __init__(self, connection_string: str) -> None:
        self._connection_string = connection_string
        self._client: QueueServiceClient | None = None

    async def _get_client(self) -> QueueServiceClient:
        if self._client is None:
            self._client = QueueServiceClient.from_connection_string(self._connection_string)
        return self._client

    async def send_queue_message(
        self,
        queue_name: str,
        message_body: dict[str, Any],
        content_type: str = "application/json",
        subject: str | None = None,
    ) -> None:
        """Send a JSON message to an Azure Storage Queue."""
        client = await self._get_client()
        queue_client = client.get_queue_client(queue_name)
        try:
            await queue_client.create_queue()
        except ResourceExistsError:
            pass
        await queue_client.send_message(json.dumps(message_body))

    async def emit_dead_letter(
        self,
        queue_name: str,
        message_body: dict[str, Any],
        content_type: str = "application/json",
        subject: str = "validation_failure",
    ) -> None:
        """Send a message to the dead-letter queue."""
        await self.send_queue_message(queue_name, message_body, content_type, subject)

    async def emit_cloudevent(
        self,
        topic_name: str,
        envelope: dict,
    ) -> None:
        """Send a CloudEvents envelope to an Azure Storage Queue."""
        client = await self._get_client()
        queue_client = client.get_queue_client(topic_name)
        try:
            await queue_client.create_queue()
        except ResourceExistsError:
            pass
        await queue_client.send_message(json.dumps(envelope))

    async def close(self) -> None:
        """Close the underlying QueueServiceClient."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> AzuriteQueueEmitter:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
