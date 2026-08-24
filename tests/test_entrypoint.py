from pipeline.__main__ import exit_code_for_status


def test_exit_code_marks_degraded_and_failed_runs_nonzero():
    assert exit_code_for_status("success") == 0
    assert exit_code_for_status("degraded") == 2
    assert exit_code_for_status("failed") == 1
