"""Table-driven tests for function_app._is_transient.

Exception type × pipeline stage (stream_upload, write_json, emit_cloudevent,
mark_completed) -> {redeliver, complete}. _is_transient classifies the raw
exception directly (walking __cause__), so the same table applies to all
four guarded call sites — the classifier does not special-case which stage
raised it, only what was raised.
"""

from __future__ import annotations

import httpx
import pytest
from azure.core.exceptions import (
    HttpResponseError,
    ResourceNotFoundError,
    ServiceRequestError,
    ServiceResponseError,
)
from azure.servicebus.exceptions import (
    MessageSizeExceededError,
    MessagingEntityNotFoundError,
    OperationTimeoutError,
    ServiceBusAuthorizationError,
    ServiceBusCommunicationError,
    ServiceBusConnectionError,
    ServiceBusServerBusyError,
)

from function_app import _is_transient
from src.adls_store import AdlsUploadError


def _http_response_error(status_code: int | None) -> HttpResponseError:
    err = HttpResponseError(message="boom")
    err.status_code = status_code
    return err


def _httpx_status_error(status_code: int) -> httpx.HTTPStatusError:
    request = httpx.Request("GET", "https://example.com/x.csv")
    response = httpx.Response(status_code=status_code, request=request)
    return httpx.HTTPStatusError("boom", request=request, response=response)


def _wrapped(cause: BaseException) -> AdlsUploadError:
    """Simulate how AdlsStore/write_json wrap the real SDK exception —
    ``raise AdlsUploadError(...) from exc`` sets ``__cause__`` exactly like
    this, without needing to actually raise/catch here."""
    wrapped = AdlsUploadError("wrapped")
    wrapped.__cause__ = cause
    return wrapped


# (exception, expected_transient) — applied identically whether this
# exception came directly out of emit_cloudevent/mark_completed (raw SDK
# exception) or wrapped in AdlsUploadError by stream_upload/write_json.
_CASES: list[tuple[BaseException, bool]] = [
    # --- transient: network / service-request level ---
    (ServiceRequestError("connection reset"), True),
    (ServiceResponseError("no response"), True),
    # --- transient: HttpResponseError 5xx/408/429 ---
    (_http_response_error(500), True),
    (_http_response_error(502), True),
    (_http_response_error(503), True),
    (_http_response_error(408), True),
    (_http_response_error(429), True),
    # --- deterministic: HttpResponseError 4xx (not 408/429), or unknown status ---
    (_http_response_error(400), False),
    (_http_response_error(403), False),
    (_http_response_error(404), False),
    (_http_response_error(409), False),
    (_http_response_error(None), False),
    (ResourceNotFoundError("not found"), False),
    # --- transient: Service Bus transport/throttle ---
    (ServiceBusConnectionError(message="disconnected"), True),
    (ServiceBusCommunicationError(message="no link"), True),
    (ServiceBusServerBusyError(message="busy"), True),
    (OperationTimeoutError(message="timed out"), True),
    # --- deterministic: Service Bus non-transport errors ---
    (MessagingEntityNotFoundError(message="no such queue"), False),
    (ServiceBusAuthorizationError(message="forbidden"), False),
    (MessageSizeExceededError(message="too big"), False),
    # --- transient: httpx transport/timeout ---
    (httpx.ConnectError("refused"), True),
    (httpx.ReadTimeout("timed out"), True),
    (httpx.ConnectTimeout("timed out"), True),
    # --- transient: httpx 5xx/408/429 status errors ---
    (_httpx_status_error(500), True),
    (_httpx_status_error(503), True),
    (_httpx_status_error(429), True),
    # --- deterministic: httpx 4xx status errors (not 408/429) ---
    (_httpx_status_error(404), False),
    (_httpx_status_error(400), False),
    # --- deterministic: anything unclassified ---
    (ValueError("bad data"), False),
    (KeyError("missing field"), False),
    (RuntimeError("bug"), False),
]


def _case_id(case: tuple[BaseException, bool]) -> str:
    exc, transient = case
    return f"{type(exc).__name__}->{'redeliver' if transient else 'complete'}"


class TestIsTransientRaw:
    """The classifier applied directly to the raw exception — this is what
    emit_cloudevent and mark_completed actually raise (no wrapper)."""

    @pytest.mark.parametrize("case", _CASES, ids=_case_id)
    def test_classification(self, case: tuple[BaseException, bool]) -> None:
        exc, expected_transient = case
        assert _is_transient(exc) is expected_transient


class TestIsTransientWrappedInAdlsUploadError:
    """The same table, but wrapped exactly as stream_upload/write_json wrap
    it (AdlsUploadError(...) from exc) — proves classification is on the
    exception itself (via __cause__), not on isinstance(AdlsUploadError)."""

    @pytest.mark.parametrize("case", _CASES, ids=_case_id)
    def test_classification(self, case: tuple[BaseException, bool]) -> None:
        exc, expected_transient = case
        wrapped = _wrapped(exc)
        assert _is_transient(wrapped) is expected_transient


class TestIsTransientSpecialCases:
    def test_missing_source_file_404_with_no_cause_is_deterministic(self) -> None:
        """stream_upload's 404 branch raises AdlsUploadError directly (no
        `from exc`) — no cause chain, must default to deterministic."""
        err = AdlsUploadError("Source not found: https://example.com/x.csv")
        assert _is_transient(err) is False

    def test_bare_adls_upload_error_no_cause_is_deterministic(self) -> None:
        assert _is_transient(AdlsUploadError("some failure")) is False
