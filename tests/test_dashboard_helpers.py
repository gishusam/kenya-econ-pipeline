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


def test_indicator_context_returns_current_previous_and_change():
    frame = pd.DataFrame(
        [
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-06-30"),
                "value": Decimal("6.4"),
            },
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-07-31"),
                "value": Decimal("6.5"),
            },
        ]
    )

    indicator_context = getattr(
        dashboard_data,
        "indicator_context",
        None,
    )

    assert indicator_context is not None

    result = indicator_context(
        frame,
        "CPI_INFLATION_YOY",
    )

    assert result["current"] == 6.5
    assert result["previous"] == 6.4
    assert round(result["change"], 1) == 0.1
    assert result["period_end"] == pd.Timestamp("2026-07-31")


def test_recent_indicator_history_limits_chart_to_latest_points():
    frame = pd.DataFrame(
        [
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2000-12-31"),
                "value": Decimal("0.6"),
            },
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2010-12-31"),
                "value": Decimal("8.1"),
            },
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2020-12-31"),
                "value": Decimal("-0.3"),
            },
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2025-12-31"),
                "value": Decimal("4.6"),
            },
        ]
    )

    recent_history = getattr(
        dashboard_data,
        "recent_indicator_history",
        None,
    )

    assert recent_history is not None

    result = recent_history(
        frame,
        "REAL_GDP_GROWTH",
        points=2,
    )

    assert list(result["period_end"]) == [
        pd.Timestamp("2020-12-31"),
        pd.Timestamp("2025-12-31"),
    ]


def test_build_economy_summary_tells_the_current_story():
    snapshot = pd.DataFrame(
        [
            {
                "indicator_code": "USD_KES",
                "value": Decimal("129.47"),
                "observation_date": pd.Timestamp("2026-08-25"),
            },
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "value": Decimal("6.5"),
                "observation_date": pd.Timestamp("2026-07-31"),
            },
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "value": Decimal("4.63261093844984"),
                "observation_date": pd.Timestamp("2025-12-31"),
            },
            {
                "indicator_code": "GDP_CURRENT_LCU",
                "value": Decimal("17577557000000"),
                "observation_date": pd.Timestamp("2025-12-31"),
            },
        ]
    )

    history = pd.DataFrame(
        [
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-06-30"),
                "value": Decimal("6.4"),
            },
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-07-31"),
                "value": Decimal("6.5"),
            },
        ]
    )

    build_summary = getattr(
        dashboard_data,
        "build_economy_summary",
        None,
    )

    assert build_summary is not None

    summary = build_summary(snapshot, history)

    assert "Inflation is 6.5%" in summary
    assert "up 0.1 percentage points" in summary
    assert "KES 129.47 per USD" in summary
    assert "Real GDP growth for 2025 is 4.6%" in summary
    assert "KES 17.58T" in summary


def test_metric_change_label_explains_fx_direction():
    label = dashboard_data.metric_change_label(
        "USD_KES",
        0.02,
    )

    assert label == "Weaker by 0.02"


def test_metric_change_label_explains_inflation_change():
    label = dashboard_data.metric_change_label(
        "CPI_INFLATION_YOY",
        0.1,
    )

    assert label == "Up 0.1 pp"


def test_metric_change_label_hides_rounding_noise():
    label = dashboard_data.metric_change_label(
        "REAL_GDP_GROWTH",
        -0.004,
    )

    assert label is None


def test_has_enough_history_requires_meaningful_sample():
    frame = pd.DataFrame(
        [
            {
                "indicator_code": "USD_KES",
                "period_end": pd.Timestamp("2026-08-24"),
                "value": Decimal("129.45"),
            },
            {
                "indicator_code": "USD_KES",
                "period_end": pd.Timestamp("2026-08-25"),
                "value": Decimal("129.47"),
            },
        ]
    )

    assert (
        dashboard_data.has_enough_history(
            frame,
            "USD_KES",
            minimum_points=7,
        )
        is False
    )


def test_annual_indicator_history_adds_categorical_year():
    frame = pd.DataFrame(
        [
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2024-12-31"),
                "value": Decimal("5.7"),
            },
            {
                "indicator_code": "REAL_GDP_GROWTH",
                "period_end": pd.Timestamp("2025-12-31"),
                "value": Decimal("4.6"),
            },
        ]
    )

    result = dashboard_data.annual_indicator_history(
        frame,
        "REAL_GDP_GROWTH",
        points=15,
    )

    assert list(result["year"]) == ["2024", "2025"]
    assert list(result["value"]) == [5.7, 4.6]


def test_indicator_context_keeps_previous_period_for_storytelling():
    frame = pd.DataFrame(
        [
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-06-30"),
                "value": Decimal("6.4"),
            },
            {
                "indicator_code": "CPI_INFLATION_YOY",
                "period_end": pd.Timestamp("2026-07-31"),
                "value": Decimal("6.5"),
            },
        ]
    )

    result = dashboard_data.indicator_context(
        frame,
        "CPI_INFLATION_YOY",
    )

    assert result["previous_period_end"] == pd.Timestamp("2026-06-30")
