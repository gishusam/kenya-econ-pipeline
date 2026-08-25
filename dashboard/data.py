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
