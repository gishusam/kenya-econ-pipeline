from __future__ import annotations

from typing import Any

import pandas as pd


def indicator_value(frame: pd.DataFrame, indicator_code: str) -> float | None:
    if frame.empty or "indicator_code" not in frame.columns:
        return None
    rows = frame.loc[frame["indicator_code"] == indicator_code, "value"]
    if rows.empty or pd.isna(rows.iloc[0]):
        return None
    return float(rows.iloc[0])


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
