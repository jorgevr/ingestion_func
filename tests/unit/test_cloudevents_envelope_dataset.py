"""Unit tests for build_dataset_envelope (R2.3; ADR 0002, ADR 0003, ADR 0004).

Covers: the deterministic CloudEvent ``id`` (unaffected by R2.3); the
correlation_id/ingestion_id split (the envelope's ``correlation_id`` is the
work item's stable value, never ``data.ingestion_id``'s fresh per-attempt
one); the newly-versioned ``type``, the ``dataschema`` attribute sourced
from the vendored contract's own ``$id``, and ``mapping_version`` now
config-driven; and that the returned envelope is validated against the
vendored ``contracts/dataset-available.v1.json`` copy before
``build_dataset_envelope`` returns it.

A redelivered work item that gets far enough to re-emit (e.g. a transient
failure struck after the previous attempt's upload but before
mark_completed ran) must resolve the same file identity + version and
therefore produce the same event id — so a downstream consumer can dedupe
by id instead of silently double-processing the same dataset. The id is
derived from ``s3_key`` (the true, always-unique source identity), not from
``site_id`` + ``category`` — ``category`` is a *derived*, non-unique
grouping (see ``extract_category``), so two genuinely different S3 objects
could otherwise collide on the same id.
"""

from __future__ import annotations

import copy

import jsonschema
import pytest

from src.cloudevents_envelope import (
    _DATASET_AVAILABLE_DATASCHEMA,
    _DATASET_AVAILABLE_VALIDATOR,
    _deterministic_event_id,
    build_dataset_envelope,
)
from src.config import HistoricalConfig

_TRACEPARENT = "00-" + "0" * 31 + "1-" + "0" * 15 + "1-01"
_TRACEPARENT_2 = "00-" + "1" * 31 + "1-" + "1" * 15 + "1-01"
_CORRELATION_ID = "550e8400-e29b-41d4-a716-446655440000"
_S3_KEY = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_data.csv"


def _historical_config(mapping_version_pvdaq: str = "unknown") -> HistoricalConfig:
    return HistoricalConfig(
        oedi_bucket_url="https://oedi-data-lake.s3.amazonaws.com",
        oedi_historical_prefix="pvdaq/2023-solar-data-prize",
        pvdaq_historical_site_ids=[9068],
        pvdaq_historical_cron_schedule="0 0 */6 * * *",
        pvdaq_historical_queue_name="pvdaq-historical-work",
        service_bus_queue_name="raw-energy-events",
        dead_letter_queue_name="pvdaq-dead-letter",
        service_bus_fully_qualified_namespace="test-sb.servicebus.windows.net",
        file_tracking_table_name="PvdaqFileTracking",
        table_storage_uri="https://teststorage.table.core.windows.net",
        data_storage_account_url="https://testaccount.blob.core.windows.net",
        bronze_container="bronze",
        tenant_id="default",
        mapping_version_pvdaq=mapping_version_pvdaq,
        schema_version_pvdaq="v1",
    )


def _data(
    site_id: int = 9068,
    category: str = "ac_power",
    version: int = 1,
    ingestion_id: str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
) -> dict:
    return {
        "site_id": site_id,
        "category": category,
        "file_format": "csv",
        "storage_path": "http://azurite/bronze/x.csv",
        "version": version,
        "ingestion_id": ingestion_id,
        "source_url": "https://oedi-data-lake.s3.amazonaws.com/x.csv",
        "file_size": 100,
        "file_hash": "a" * 64,
    }


def _build(
    *,
    config: HistoricalConfig | None = None,
    correlation_id: str = _CORRELATION_ID,
    traceparent: str = _TRACEPARENT,
    ingestion_timestamp: str = "2026-01-01T00:00:00+00:00",
    s3_key: str = _S3_KEY,
    **data_overrides,
) -> dict:
    return build_dataset_envelope(
        data=_data(**data_overrides),
        config=config or _historical_config(),
        correlation_id=correlation_id,
        traceparent=traceparent,
        ingestion_timestamp=ingestion_timestamp,
        s3_key=s3_key,
    )


class TestDeterministicEventId:
    def test_same_identity_same_id(self) -> None:
        assert _deterministic_event_id(_S3_KEY, 1) == _deterministic_event_id(
            _S3_KEY, 1
        )

    def test_different_version_different_id(self) -> None:
        assert _deterministic_event_id(_S3_KEY, 1) != _deterministic_event_id(
            _S3_KEY, 2
        )

    def test_different_s3_key_different_id(self) -> None:
        other_key = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_irradiance.csv"
        assert _deterministic_event_id(_S3_KEY, 1) != _deterministic_event_id(
            other_key, 1
        )

    def test_id_is_a_valid_uuid_string(self) -> None:
        import uuid

        result = _deterministic_event_id(_S3_KEY, 1)
        # Round-trips through UUID() without raising, and is lowercase canonical.
        assert str(uuid.UUID(result)) == result

    def test_two_split_files_with_same_category_yield_different_ids(self) -> None:
        """The review's motivating scenario: a single logical dataset split
        across two distinct S3 objects that both extract to the same
        site_id+category grouping. Keying on s3_key (not site_id+category)
        is what keeps their event ids from colliding."""
        key_a = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part1.csv"
        key_b = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part2.csv"
        assert _deterministic_event_id(key_a, 1) != _deterministic_event_id(key_b, 1)


class TestBuildDatasetEnvelopeUsesDeterministicId:
    def test_two_calls_with_same_data_produce_the_same_envelope_id(self) -> None:
        # Simulate a redelivered retry: a fresh correlation_id/timestamp
        # would be wrong (see TestCorrelationIdStability below) — here only
        # the irrelevant traceparent/timestamp vary, same file identity +
        # version, and the id still matches.
        envelope1 = _build()
        envelope2 = _build(
            traceparent=_TRACEPARENT_2, ingestion_timestamp="2026-01-01T00:05:00+00:00"
        )

        assert envelope1["id"] == envelope2["id"]
        assert envelope1["id"] == _deterministic_event_id(_S3_KEY, 1)

    def test_different_version_produces_different_envelope_id(self) -> None:
        envelope_v1 = _build(version=1)
        envelope_v2 = _build(version=2)
        assert envelope_v1["id"] != envelope_v2["id"]

    def test_different_s3_key_produces_different_envelope_id(self) -> None:
        """Same site_id+category, two distinct S3 objects (the split-file
        review scenario) — must not collide on the same envelope id."""
        key_a = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part1.csv"
        key_b = "pvdaq/2023-solar-data-prize/9068_OEDI/data/9068_ac_power_part2.csv"
        envelope_a = _build(s3_key=key_a)
        envelope_b = _build(s3_key=key_b)
        assert envelope_a["id"] != envelope_b["id"]


class TestCorrelationIdStability:
    """R2.3's explicit acceptance: the envelope's correlation_id is the work
    item's own value — stable across Service Bus redelivery of that same
    message — and is never conflated with data.ingestion_id, a fresh
    per-attempt identifier."""

    def test_envelope_correlation_id_is_the_passed_correlation_id(self) -> None:
        envelope = _build(
            correlation_id=_CORRELATION_ID,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        assert envelope["correlation_id"] == _CORRELATION_ID

    def test_correlation_id_differs_from_data_ingestion_id(self) -> None:
        """The two are deliberately distinct fields — a test fixture where
        they happen to be equal would not catch a regression that
        conflates them."""
        envelope = _build(
            correlation_id=_CORRELATION_ID,
            ingestion_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        )
        assert envelope["correlation_id"] != envelope["data"]["ingestion_id"]
        assert envelope["correlation_id"] == _CORRELATION_ID
        assert (
            envelope["data"]["ingestion_id"] == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
        )

    def test_correlation_id_stays_identical_across_simulated_redelivery(self) -> None:
        """A Service Bus redelivery of the identical work item: the message
        body (and therefore the work item's correlation_id) does not
        change, but a fresh attempt gets its own ingestion_id, traceparent
        and timestamp. The envelope's correlation_id must track the
        former, never the latter."""
        first_attempt = _build(
            correlation_id=_CORRELATION_ID,
            ingestion_id="11111111-1111-1111-1111-111111111111",
            traceparent=_TRACEPARENT,
            ingestion_timestamp="2026-01-01T00:00:00+00:00",
        )
        redelivered_attempt = _build(
            correlation_id=_CORRELATION_ID,
            ingestion_id="22222222-2222-2222-2222-222222222222",
            traceparent=_TRACEPARENT_2,
            ingestion_timestamp="2026-01-01T00:05:00+00:00",
        )

        assert first_attempt["correlation_id"] == redelivered_attempt["correlation_id"]
        assert first_attempt["correlation_id"] == _CORRELATION_ID
        # ...while the per-attempt identifier legitimately changed.
        assert (
            first_attempt["data"]["ingestion_id"]
            != redelivered_attempt["data"]["ingestion_id"]
        )


class TestVersionedTypeDataschemaAndMappingVersion:
    """R2.3 Output: type gains its major-version suffix, dataschema is new
    and sourced from the vendored contract's own $id (never hardcoded
    separately, so it cannot drift from it), and mapping_version comes from
    configuration instead of a hardcoded literal."""

    def test_type_is_versioned(self) -> None:
        envelope = _build()
        assert envelope["type"] == "solar.pvdaq.dataset.available.v1"

    def test_dataschema_equals_the_vendored_contracts_own_id(self) -> None:
        envelope = _build()
        assert envelope["dataschema"] == _DATASET_AVAILABLE_DATASCHEMA
        assert envelope["dataschema"] == _DATASET_AVAILABLE_VALIDATOR.schema["$id"]

    def test_mapping_version_comes_from_config_not_hardcoded(self) -> None:
        """Proven with a non-'unknown' value: if the literal were still
        hardcoded, this would still read 'unknown' and the assertion would
        fail."""
        envelope = _build(config=_historical_config(mapping_version_pvdaq="v3"))
        assert envelope["mapping_version"] == "v3"

    def test_mapping_version_unknown_sentinel_still_passes_through(self) -> None:
        envelope = _build(config=_historical_config(mapping_version_pvdaq="unknown"))
        assert envelope["mapping_version"] == "unknown"


class TestBuildDatasetEnvelopeValidatesAgainstContract:
    """R2.3 Accept: every emitted envelope validates against the vendored
    contract, and an envelope with an unknown EXTRA field fails that
    validation — but an additive field inside the open `data` block does
    not."""

    def test_build_dataset_envelope_returns_a_valid_envelope(self) -> None:
        envelope = _build()
        # Does not raise.
        _DATASET_AVAILABLE_VALIDATOR.validate(envelope)

    def test_unknown_envelope_attribute_fails_validation(self) -> None:
        envelope = _build()
        envelope["unexpected_extra_attribute"] = "should not be here"
        with pytest.raises(jsonschema.ValidationError):
            _DATASET_AVAILABLE_VALIDATOR.validate(envelope)

    def test_additive_data_field_still_validates(self) -> None:
        """data is open by construction (ADR 0002 rule 2) — an extra field
        there is a legal minor version, not a violation."""
        envelope = _build()
        envelope = copy.deepcopy(envelope)
        envelope["data"]["device_id"] = None
        # Does not raise.
        _DATASET_AVAILABLE_VALIDATOR.validate(envelope)

    def test_build_dataset_envelope_raises_on_incomplete_data_block(self) -> None:
        """Proves validation is load-bearing inside build_dataset_envelope
        itself (R2.3: "validated against the vendored contract before
        send"), not merely available for tests to call separately — a
        caller-supplied data block missing a required field must fail
        right here, before anything is ever sent."""
        incomplete_data = _data()
        del incomplete_data["file_hash"]

        with pytest.raises(jsonschema.ValidationError):
            build_dataset_envelope(
                data=incomplete_data,
                config=_historical_config(),
                correlation_id=_CORRELATION_ID,
                traceparent=_TRACEPARENT,
                ingestion_timestamp="2026-01-01T00:00:00+00:00",
                s3_key=_S3_KEY,
            )
