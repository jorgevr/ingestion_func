"""CI-3 throwaway scratch file — scenario (b): a deliberately skipped test.
Reverted immediately after the proof run; never meant to land."""

import pytest


@pytest.mark.skip(reason="CI-3 scenario (b): deliberate skip for gate proof")
def test_ci3_scenario_b_deliberate_skip() -> None:
    assert True
