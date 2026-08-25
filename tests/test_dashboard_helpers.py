import pandas as pd
from decimal import Decimal
from dashboard import data as dashboard_data
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

def test_display_number_converts_decimal_for_streamlit():
    display_number = getattr(dashboard_data, "display_number", None)

    assert display_number is not None
    assert display_number(Decimal("129.47")) == 129.47
    assert display_number(Decimal("6.5")) == 6.5

def test_prepare_snapshot_for_display_formats_dashboard_values():
    frame = pd.DataFrame(
        [
            {
                "source": "WORLD_BANK",
                "value": Decimal("4.63261093844984"),
                "ingested_at": pd.Timestamp("2026-08-25T09:09:03Z"),
            }
        ]
    )

    prepare = getattr(dashboard_data, "prepare_snapshot_for_display", None)

    assert prepare is not None

    result = prepare(frame)

    assert result.loc[0, "source"] == "World Bank"
    assert result.loc[0, "value"] == 4.63261093844984
    assert result.loc[0, "ingested_at"] == "25 Aug 2026 · 12:09 EAT"


def test_prepare_health_for_display_formats_source_and_check_time():
    frame = pd.DataFrame(
        [
            {
                "source": "WORLD_BANK",
                "freshness_status": "CURRENT",
                "last_checked_at": pd.Timestamp("2026-08-25T09:09:13Z"),
            }
        ]
    )

    prepare = getattr(dashboard_data, "prepare_health_for_display", None)

    assert prepare is not None

    result = prepare(frame)

    assert result.loc[0, "source"] == "World Bank"
    assert result.loc[0, "last_checked_at"] == "25 Aug 2026 · 12:09 EAT"
