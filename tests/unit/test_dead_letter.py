"""R2.1d item 4 / R2.1e item 2: ``_dead_letter`` is the single choke point
for every dead-letter ``historical_worker`` sends. If the send itself
fails, this must log an ERROR naming the reason and correlation_id and
raise ``DeadLetterSendError`` from the original — the original message is
then never completed and is redelivered instead (bounded by the queue's
maxDeliveryCount). Losing the dead-letter send must never silently look
like "handled". ``DeadLetterSendError`` is a distinct type specifically so
the caller (historical_worker) can tell "the send failed" apart from any
other failure and avoid attempting a second dead-letter send for the same
delivery — see test_historical_pipeline.py's
TestDeadLetterSendFailureDoesNotRetry for that half of the contract.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from azure.servicebus.exceptions import MessagingEntityNotFoundError

from function_app import DeadLetterSendError, _dead_letter


def _fake_config() -> SimpleNamespace:
    return SimpleNamespace(dead_letter_queue_name="pvdaq-dead-letter")


class _FakeEmitterCtx:
    """Minimal async-context-manager emitter whose emit_dead_letter fails."""

    def __init__(self, emit_dead_letter_side_effect: Exception) -> None:
        self.emit_dead_letter = AsyncMock(side_effect=emit_dead_letter_side_effect)

    async def __aenter__(self) -> "_FakeEmitterCtx":
        return self

    async def __aexit__(self, *exc_info) -> None:
        return None


class TestDeadLetterSendFailureEscalates:
    @pytest.mark.asyncio
    async def test_send_failure_raises_and_logs_error(self) -> None:
        underlying = MessagingEntityNotFoundError(message="no such queue")
        fake_emitter = _FakeEmitterCtx(underlying)
        logger = MagicMock()

        with patch("function_app._make_emitter", return_value=fake_emitter):
            with pytest.raises(DeadLetterSendError) as exc_info:
                await _dead_letter(
                    _fake_config(),
                    logger,
                    correlation_id="corr-123",
                    reason="dataset_failure",
                    detail={"file_reference": "x.csv", "failure_reason": "boom"},
                )

        # Chained from the original SDK exception, not swallowed.
        assert isinstance(exc_info.value.__cause__, MessagingEntityNotFoundError)

        fake_emitter.emit_dead_letter.assert_called_once()
        logger.error.assert_called_once()
        args, kwargs = logger.error.call_args
        # "reason=%s correlation_id=%s" positional-format call.
        assert "dataset_failure" in args
        assert "corr-123" in args
        assert kwargs.get("exc_info") is True

    @pytest.mark.asyncio
    async def test_successful_send_does_not_log_error(self) -> None:
        fake_emitter = _FakeEmitterCtx(RuntimeError("unused"))
        fake_emitter.emit_dead_letter = AsyncMock(return_value=None)
        logger = MagicMock()

        with patch("function_app._make_emitter", return_value=fake_emitter):
            await _dead_letter(
                _fake_config(),
                logger,
                correlation_id="corr-456",
                reason="malformed_body",
                detail={"file_reference": "preview", "failure_reason": "bad json"},
            )

        fake_emitter.emit_dead_letter.assert_called_once()
        logger.error.assert_not_called()

    @pytest.mark.asyncio
    async def test_send_failure_body_still_carries_reason_and_detail(self) -> None:
        """Even though the send itself fails, the attempted body is built
        correctly — proves the failure is in the transport, not the body."""
        underlying = MessagingEntityNotFoundError(message="no such queue")
        fake_emitter = _FakeEmitterCtx(underlying)
        logger = MagicMock()

        with (
            patch("function_app._make_emitter", return_value=fake_emitter),
            pytest.raises(DeadLetterSendError),
        ):
            await _dead_letter(
                _fake_config(),
                logger,
                correlation_id="corr-789",
                reason="validation_failure",
                detail={"file_reference": "raw body", "failure_reason": "schema"},
            )

        call_kwargs = fake_emitter.emit_dead_letter.call_args.kwargs
        assert call_kwargs["queue_name"] == "pvdaq-dead-letter"
        assert call_kwargs["message_body"]["correlation_id"] == "corr-789"
        assert call_kwargs["message_body"]["error_type"] == "validation_failure"
        assert call_kwargs["message_body"]["file_reference"] == "raw body"
        assert call_kwargs["subject"] == "validation_failure"
