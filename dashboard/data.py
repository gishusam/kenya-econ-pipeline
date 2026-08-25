from __future__ import annotations

from decimal import Decimal
from typing import Any

import pandas as pd


def indicator_value(frame: pd.DataFrame, indicator_code: str) -> float | None:
    if frame.empty or "indicator_code" not in frame.columns:
        return None
    rows = frame.loc[frame["indicator_code"] == indicator_code, "value"]
    if rows.empty or pd.isna(rows.iloc[0]):
        return None
    return float(rows.iloc[0])


def display_number(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value


def display_source_name(value: str) -> str:
    return {
        "WORLD_BANK": "World Bank",
        "KNBS": "KNBS",
        "CBK": "CBK",
    }.get(value, value)


def format_eat_timestamp(value) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None

    timestamp = timestamp.tz_convert("Africa/Nairobi")
    return timestamp.strftime("%d %b %Y · %H:%M EAT")


def prepare_snapshot_for_display(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()

    if "source" in result.columns:
        result["source"] = result["source"].map(display_source_name)

    if "value" in result.columns:
        result["value"] = result["value"].map(display_number)

    if "ingested_at" in result.columns:
        result["ingested_at"] = result["ingested_at"].map(format_eat_timestamp)

    return result


def prepare_health_for_display(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()

    if "source" in result.columns:
        result["source"] = result["source"].map(display_source_name)

    if "last_checked_at" in result.columns:
        result["last_checked_at"] = result["last_checked_at"].map(format_eat_timestamp)

    return result


def status_icon(status: str) -> str:
    return {"CURRENT": "●", "STALE": "◐", "DEGRADED": "▲"}.get(status, "?")


def build_bigquery_client(project_id: str, service_account_info: dict[str, Any] | None = None):
    from google.cloud import bigquery

    if service_account_info:
        from google.oauth2 import service_account

        credentials = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project_id, credentials=credentials)

    return bigquery.Client(project=project_id)


def query_dataframe(client, sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()


def indicator_context(
    frame: pd.DataFrame,
    indicator_code: str,
) -> dict[str, Any]:
    rows = frame[
        frame["indicator_code"] == indicator_code
    ].copy()

    if rows.empty:
        return {
            "current": None,
            "previous": None,
            "change": None,
            "period_end": None,
            "previous_period_end": None,
        }

    rows = rows.sort_values("period_end")

    current_row = rows.iloc[-1]
    current = display_number(current_row["value"])

    previous = None
    previous_period_end = None
    change = None

    if len(rows) > 1:
        previous_row = rows.iloc[-2]
        previous = display_number(previous_row["value"])
        previous_period_end = previous_row["period_end"]

        if current is not None and previous is not None:
            change = current - previous

    return {
        "current": current,
        "previous": previous,
        "change": change,
        "period_end": current_row["period_end"],
        "previous_period_end": previous_period_end,
    }


def recent_indicator_history(
    frame: pd.DataFrame,
    indicator_code: str,
    points: int = 12,
) -> pd.DataFrame:
    rows = frame[
        frame["indicator_code"] == indicator_code
    ].copy()

    rows = rows.sort_values("period_end")

    return rows.tail(points).reset_index(drop=True)


def build_economy_summary(
    snapshot: pd.DataFrame,
    history: pd.DataFrame,
) -> str:
    inflation = indicator_value(
        snapshot,
        "CPI_INFLATION_YOY",
    )
    usd_kes = indicator_value(
        snapshot,
        "USD_KES",
    )
    gdp_growth = indicator_value(
        snapshot,
        "REAL_GDP_GROWTH",
    )
    gdp_lcu = indicator_value(
        snapshot,
        "GDP_CURRENT_LCU",
    )

    inflation_context = indicator_context(
        history,
        "CPI_INFLATION_YOY",
    )

    inflation_change = inflation_context["change"]

    if inflation_change is None:
        inflation_change_text = ""
    elif inflation_change > 0:
        inflation_change_text = (
            f", up {inflation_change:.1f} percentage points "
            "from the previous observation"
        )
    elif inflation_change < 0:
        inflation_change_text = (
            f", down {abs(inflation_change):.1f} percentage points "
            "from the previous observation"
        )
    else:
        inflation_change_text = (
            ", unchanged from the previous observation"
        )

    gdp_rows = snapshot[
        snapshot["indicator_code"] == "REAL_GDP_GROWTH"
    ]

    gdp_year = None
    if not gdp_rows.empty:
        observation_date = pd.to_datetime(
            gdp_rows.iloc[0]["observation_date"],
            errors="coerce",
        )
        if pd.notna(observation_date):
            gdp_year = observation_date.year

    parts = []

    if inflation is not None:
        parts.append(
            f"Inflation is {inflation:.1f}%"
            f"{inflation_change_text}."
        )

    if usd_kes is not None:
        parts.append(
            f"The shilling is at KES {usd_kes:.2f} per USD."
        )

    if gdp_growth is not None:
        year_text = (
            f" for {gdp_year}"
            if gdp_year is not None
            else ""
        )
        parts.append(
            f"Real GDP growth{year_text} is "
            f"{gdp_growth:.1f}%."
        )

    if gdp_lcu is not None:
        parts.append(
            f"Nominal GDP is approximately "
            f"KES {gdp_lcu / 1e12:.2f}T."
        )

    return " ".join(parts)


def metric_change_label(
    indicator_code: str,
    change: float | None,
) -> str | None:
    if change is None or pd.isna(change):
        return None

    change = float(change)

    if indicator_code == "USD_KES":
        if abs(change) < 0.005:
            return None

        direction = "Weaker" if change > 0 else "Stronger"
        return f"{direction} by {abs(change):.2f}"

    if indicator_code in {
        "CPI_INFLATION_YOY",
        "REAL_GDP_GROWTH",
    }:
        # At one decimal place anything below 0.05pp
        # is display noise such as -0.0pp.
        if abs(change) < 0.05:
            return None

        direction = "Up" if change > 0 else "Down"
        return f"{direction} {abs(change):.1f} pp"

    if abs(change) < 0.005:
        return None

    direction = "Up" if change > 0 else "Down"
    return f"{direction} {abs(change):.2f}"


def has_enough_history(
    frame: pd.DataFrame,
    indicator_code: str,
    minimum_points: int = 7,
) -> bool:
    rows = frame[
        frame["indicator_code"] == indicator_code
    ]

    if "value" in rows.columns:
        rows = rows[rows["value"].notna()]

    return len(rows) >= minimum_points


def annual_indicator_history(
    frame: pd.DataFrame,
    indicator_code: str,
    points: int = 15,
) -> pd.DataFrame:
    result = recent_indicator_history(
        frame,
        indicator_code,
        points=points,
    ).copy()

    if result.empty:
        result["year"] = pd.Series(dtype="object")
        return result

    result["period_end"] = pd.to_datetime(
        result["period_end"],
        errors="coerce",
    )

    result["year"] = result["period_end"].dt.year.astype(
        "Int64"
    ).astype(str)

    result["value"] = result["value"].map(display_number)

    return result
