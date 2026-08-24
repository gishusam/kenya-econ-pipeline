import pandas as pd

from dashboard.data import indicator_value, status_icon


def test_indicator_value_returns_none_when_indicator_is_missing():
    frame = pd.DataFrame([{"indicator_code": "USD_KES", "value": 129.47}])
    assert indicator_value(frame, "CPI_INFLATION_YOY") is None


def test_indicator_value_returns_numeric_value_for_requested_indicator():
    frame = pd.DataFrame([{"indicator_code": "USD_KES", "value": 129.47}])
    assert indicator_value(frame, "USD_KES") == 129.47


def test_status_icon_distinguishes_current_stale_and_degraded():
    assert status_icon("CURRENT") == "●"
    assert status_icon("STALE") == "◐"
    assert status_icon("DEGRADED") == "▲"
