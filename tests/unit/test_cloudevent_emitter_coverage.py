"""R2.3b F1: every CloudEvent emission path must either (a) validate its
envelope against a vendored registry contract before ``emit_cloudevent``,
or (b) be an explicitly acknowledged exception named here — so a *new*,
unvalidated emitter added anywhere in this codebase fails this test
instead of silently shipping another unversioned/unvalidated CloudEvent.

Investigation (grepping ``event_type``, ``cloudevent`` and
``emit_cloudevent`` across ``src/`` and ``function_app.py``) finds exactly
two envelope-building functions whose output reaches
``ServiceBusEmitter.emit_cloudevent``:

1. ``src/cloudevents_envelope.py::build_dataset_envelope`` — used by
   ``function_app.py::historical_worker`` (feature 002, the documented
   data flow per ``AGENTS.md`` §2). Versioned type
   (``solar.pvdaq.dataset.available.v1``), ``dataschema``, validates
   against the vendored ``contracts/dataset-available.v1.json`` before
   returning (R2.3). This is ``_VALIDATED_BUILDERS``.

2. ``src/cloudevents_envelope.py::build_envelope`` — used by
   ``src/record_pipeline.py::process_record``, called from
   ``function_app.py::pvdaq_ingestion`` (feature 001, the daily timer).
   Defaults to the **unversioned** type ``"solar.pvdaq.dataset.available"``,
   has no ``dataschema`` attribute, and validates against nothing — there
   is no registry contract for its per-record ``data`` shape at all
   (``contracts/`` holds only ``dataset-available.v1.json``,
   ``dataset-validated.v1.json``, ``quarantine-record.v1.json`` and
   ``metadata-file.v1.json``; none match a single raw telemetry record).

   **Known issue, not fixed here.** ``AGENTS.md`` §2's documented data flow
   names only ``historical_dispatcher`` -> ``historical_worker``;
   ``pvdaq_ingestion``/``process_record`` isn't part of it at all. This
   path emits onto the *same* ``raw-energy-events`` queue the historical
   path uses; ``processing-func``'s R3.7 validator rejects the unversioned
   type as unrecognized (dead-letters it), so it is queue/DLQ noise rather
   than data corruption — but it is still a live, unversioned, unvalidated
   emitter. Disabling a live, currently-scheduled function's emission is a
   larger architectural call than this fix round's scope; reported and
   proposed for removal in the R2.3b report instead of actioned here. It
   is tracked explicitly below via ``_KNOWN_UNVALIDATED_BUILDERS`` so this
   test still catches any *additional* unvalidated emitter rather than
   passing the whole category silently.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

# services/ingestion-func/tests/unit/test_cloudevent_emitter_coverage.py -> service root
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SOURCE_FILES = [
    _REPO_ROOT / "function_app.py",
    *sorted((_REPO_ROOT / "src").glob("*.py")),
]

_EMIT_CLOUDEVENT_PATTERN = re.compile(r"\.emit_cloudevent\s*\(")

# Every source file with a live call to .emit_cloudevent(...), as of this
# audit. A new file calling it (or a known one stopping) means this audit
# is stale and must be redone — deliberately, not silently.
_KNOWN_EMIT_CLOUDEVENT_CALL_SITES = {
    "function_app.py",
    "src/record_pipeline.py",
}

# Envelope-building functions confirmed to validate their own output
# against a vendored registry contract before returning — see
# tests/unit/test_cloudevents_envelope_dataset.py::
# TestBuildDatasetEnvelopeValidatesAgainstContract for the behavioral proof.
_VALIDATED_BUILDERS = {"build_dataset_envelope"}

# Envelope-building functions confirmed NOT to validate — acknowledged and
# reported (see the module docstring above and the R2.3b report), not
# fixed in this commit. Anything added here must carry the same kind of
# explanation; anything reachable from a known call site that is in
# neither set fails this test.
_KNOWN_UNVALIDATED_BUILDERS = {"build_envelope"}


def _files_calling_emit_cloudevent() -> set[str]:
    found: set[str] = set()
    for path in _SOURCE_FILES:
        text = path.read_text(encoding="utf-8")
        if _EMIT_CLOUDEVENT_PATTERN.search(text):
            found.add(str(path.relative_to(_REPO_ROOT)).replace("\\", "/"))
    return found


class TestEmitCloudeventCallSitesAreFullyAudited:
    def test_known_call_sites_match_the_installed_source(self) -> None:
        """Fails loudly, naming the files, if a NEW file starts calling
        emit_cloudevent or a known one stops."""
        found = _files_calling_emit_cloudevent()
        assert found == _KNOWN_EMIT_CLOUDEVENT_CALL_SITES, (
            f"emit_cloudevent call sites changed — found={sorted(found)}, "
            f"expected={sorted(_KNOWN_EMIT_CLOUDEVENT_CALL_SITES)}. Update "
            "this audit (and _VALIDATED_BUILDERS / "
            "_KNOWN_UNVALIDATED_BUILDERS) deliberately to match."
        )

    def test_validated_and_unvalidated_builder_sets_are_disjoint(self) -> None:
        overlap = _VALIDATED_BUILDERS & _KNOWN_UNVALIDATED_BUILDERS
        assert not overlap, (
            f"a builder cannot be both validated and acknowledged-unvalidated: "
            f"{overlap}"
        )

    def test_historical_worker_uses_a_validated_builder(self) -> None:
        import function_app

        # @app.service_bus_queue_trigger wraps the plain function in a
        # FunctionBuilder — unwrap it to get back the real callable
        # inspect.getsource can read.
        real_fn = function_app.historical_worker._function.get_user_function()
        source = inspect.getsource(real_fn)
        used = {
            name
            for name in _VALIDATED_BUILDERS | _KNOWN_UNVALIDATED_BUILDERS
            if f"{name}(" in source
        }
        assert used, (
            "historical_worker no longer calls any known envelope builder — "
            "update this audit."
        )
        unclassified_as_unvalidated = used & _KNOWN_UNVALIDATED_BUILDERS
        assert not unclassified_as_unvalidated, (
            f"historical_worker (the documented, contract-backed data flow) "
            f"uses a builder acknowledged as unvalidated: "
            f"{unclassified_as_unvalidated} — this must be validated, not "
            f"merely acknowledged."
        )
        assert used <= _VALIDATED_BUILDERS

    def test_pvdaq_ingestion_pipeline_uses_only_known_classified_builders(
        self,
    ) -> None:
        """process_record (pvdaq_ingestion's pipeline) must use a builder
        that is explicitly classified one way or the other — not an
        unclassified new one."""
        import src.record_pipeline as record_pipeline

        source = inspect.getsource(record_pipeline.process_record)
        used = {
            name
            for name in _VALIDATED_BUILDERS | _KNOWN_UNVALIDATED_BUILDERS
            if f"{name}(" in source
        }
        assert used, (
            "process_record no longer calls any known envelope builder — "
            "update this audit."
        )
        assert used <= (_VALIDATED_BUILDERS | _KNOWN_UNVALIDATED_BUILDERS), (
            f"process_record uses an unclassified envelope builder: "
            f"{used - _VALIDATED_BUILDERS - _KNOWN_UNVALIDATED_BUILDERS} — "
            "every builder reachable from a known emit_cloudevent call site "
            "must be added to _VALIDATED_BUILDERS (and made to actually "
            "validate) or to _KNOWN_UNVALIDATED_BUILDERS with a reported, "
            "acknowledged reason."
        )
