"""R2.9a guard tests (Part A item 4).

a) function_app registers exactly the expected set of function names, and
   pvdaq_ingestion is specifically not among them — the retired daily
   timer function must not still be wired up to the host.
b) the retired, unversioned event type string ``solar.pvdaq.dataset.available``
   (not followed by ``.v1``) must not appear anywhere under src/ or
   function_app.py — a regression here would mean the retired type leaked
   back into a live code path.
"""

from __future__ import annotations

import re
from pathlib import Path

import function_app

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

# Every function name the host should register, post-R2.9. pvdaq_ingestion
# (feature 001, daily polling) is retired and must never reappear here.
_EXPECTED_FUNCTION_NAMES = {"historical_dispatcher", "historical_worker"}

# azure.functions' FunctionApp.get_functions() is not safe to call more than
# once per process: it mutates self.functions_bindings and re-validates on
# every call, so a second call in the same process raises ValueError
# ("does not have a unique function name") even though nothing changed.
# Call it exactly once here and have every test read this frozen result.
_REGISTERED_FUNCTION_NAMES = {
    fn.get_function_name() for fn in function_app.app.get_functions()
}

# Matches the unversioned retired type exactly — "solar.pvdaq.dataset.available"
# NOT immediately followed by ".v1" (a word boundary after "available" that
# isn't a literal ".v1"). Uses a negative lookahead so
# "solar.pvdaq.dataset.available.v1" does not match, but
# "solar.pvdaq.dataset.available" (bare, or followed by anything else) does.
_RETIRED_TYPE_PATTERN = re.compile(r"solar\.pvdaq\.dataset\.available(?!\.v1)")


class TestFunctionRegistration:
    def test_registered_functions_match_expected_set_exactly(self) -> None:
        assert _REGISTERED_FUNCTION_NAMES == _EXPECTED_FUNCTION_NAMES, (
            f"registered={sorted(_REGISTERED_FUNCTION_NAMES)} "
            f"expected={sorted(_EXPECTED_FUNCTION_NAMES)}"
        )

    def test_pvdaq_ingestion_is_not_registered(self) -> None:
        assert "pvdaq_ingestion" not in _REGISTERED_FUNCTION_NAMES

    def test_pvdaq_ingestion_attribute_does_not_exist(self) -> None:
        assert not hasattr(function_app, "pvdaq_ingestion")


class TestRetiredEventTypeStringAbsent:
    def test_no_unversioned_retired_type_under_src_or_function_app(self) -> None:
        offenders: list[str] = []
        files = [
            _REPO_ROOT / "function_app.py",
            *sorted((_REPO_ROOT / "src").glob("*.py")),
        ]
        for path in files:
            text = path.read_text(encoding="utf-8")
            if _RETIRED_TYPE_PATTERN.search(text):
                offenders.append(str(path.relative_to(_REPO_ROOT)).replace("\\", "/"))

        assert not offenders, (
            "retired unversioned type 'solar.pvdaq.dataset.available' "
            f"(without .v1) found in: {offenders}"
        )
