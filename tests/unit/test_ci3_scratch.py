"""CI-3 throwaway scratch file — scenario (a): a deliberately failing test.
Reverted immediately after the proof run; never meant to land."""


def test_ci3_scenario_a_deliberate_failure() -> None:
    assert False, "CI-3 scenario (a): deliberate failure for gate proof"
