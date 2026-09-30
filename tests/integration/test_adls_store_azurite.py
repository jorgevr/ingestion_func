"""R2.1e item 1(b): proves the atomic identity guard (AdlsStore.stream_upload
-> _commit_with_identity_guard) actually closes the race under real
concurrency, against a real Azurite instance — not just mocked SDK calls.

Skipped unless AZURITE_TEST_CONNECTION is set to a live Azurite connection
string (e.g. the docker-compose stack's Azurite service, or
``UseDevelopmentStorage=true`` against a locally running one). Run with:

    AZURITE_TEST_CONNECTION="UseDevelopmentStorage=true" \
        pytest tests/integration/test_adls_store_azurite.py -m azurite
"""

from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest

from src.adls_store import AdlsBlobIdentityMismatchError, AdlsStore

pytestmark = pytest.mark.azurite

_AZURITE_CONNECTION = os.environ.get("AZURITE_TEST_CONNECTION")

skip_without_azurite = pytest.mark.skipif(
    not _AZURITE_CONNECTION,
    reason="AZURITE_TEST_CONNECTION not set — set it to a live Azurite "
    "connection string to run this test against real storage",
)


def _fake_http_client(body: bytes) -> httpx.AsyncClient:
    """A real httpx.AsyncClient pointed at a data: transport isn't
    available, so this uses httpx's MockTransport to serve a fixed body
    without touching the network — the point of this test is exercising
    the real Azure Storage SDK's conditional-commit race, not S3."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=body)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@skip_without_azurite
class TestConcurrentUploadsToSamePath:
    @pytest.mark.asyncio
    async def test_two_different_s3_keys_race_exactly_one_wins(self) -> None:
        container = f"test-identity-guard-{uuid.uuid4().hex[:8]}"
        file_path = f"race-test/{uuid.uuid4().hex}.csv"
        key_a = f"pvdaq/site/data/{uuid.uuid4().hex}_part1.csv"
        key_b = f"pvdaq/site/data/{uuid.uuid4().hex}_part2.csv"

        store_a = AdlsStore(
            account_url="unused",
            container_name=container,
            connection_string=_AZURITE_CONNECTION,
        )
        store_b = AdlsStore(
            account_url="unused",
            container_name=container,
            connection_string=_AZURITE_CONNECTION,
        )

        try:
            results = await asyncio.gather(
                store_a.stream_upload(
                    source_url="https://example.invalid/a.csv",
                    file_path=file_path,
                    s3_key=key_a,
                    http_client=_fake_http_client(b"a,b,c\n1,2,3\n"),
                ),
                store_b.stream_upload(
                    source_url="https://example.invalid/b.csv",
                    file_path=file_path,
                    s3_key=key_b,
                    http_client=_fake_http_client(b"x,y,z\n4,5,6\n"),
                ),
                return_exceptions=True,
            )

            successes = [r for r in results if not isinstance(r, BaseException)]
            failures = [r for r in results if isinstance(r, BaseException)]

            assert len(successes) == 1, (
                f"expected exactly one winner, got results={results!r}"
            )
            assert len(failures) == 1
            assert isinstance(failures[0], AdlsBlobIdentityMismatchError)

            winner_key = key_a if not isinstance(results[0], BaseException) else key_b

            # The blob's metadata must match whichever upload actually won —
            # never a mix, never the loser's identity.
            blob_svc = await store_a._get_client()
            blob_client = blob_svc.get_blob_client(container=container, blob=file_path)
            props = await blob_client.get_blob_properties()
            assert props.metadata.get("s3_key") == winner_key
        finally:
            await store_a.close()
            await store_b.close()
            # Best-effort cleanup of the scratch container.
            cleanup_store = AdlsStore(
                account_url="unused",
                container_name=container,
                connection_string=_AZURITE_CONNECTION,
            )
            try:
                blob_svc = await cleanup_store._get_client()
                await blob_svc.delete_container(container)
            except Exception:
                pass
            finally:
                await cleanup_store.close()
