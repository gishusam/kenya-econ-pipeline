from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.data import build_bigquery_client, indicator_value, query_dataframe, status_icon

st.set_page_config(page_title="Kenya Economic Intelligence", page_icon="🇰🇪", layout="wide")


def _secret(name: str, default=None):
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


@st.cache_resource
def get_client():
    project_id = _secret("GCP_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("Set GCP_PROJECT_ID in Streamlit secrets or the environment")
    service_account_info = _secret("gcp_service_account")
    if service_account_info:
        service_account_info = dict(service_account_info)
    return build_bigquery_client(project_id, service_account_info), project_id


@st.cache_data(ttl=300)
def load_dashboard_data():
    client, project_id = get_client()
    snapshot = query_dataframe(client, f"SELECT * FROM `{project_id}.marts.economic_snapshot`")
    history = query_dataframe(
        client,
        f"SELECT * FROM `{project_id}.marts.indicator_history` ORDER BY period_end",
    )
    health = query_dataframe(client, f"SELECT * FROM `{project_id}.marts.source_health` ORDER BY source")
    pipeline = query_dataframe(client, f"SELECT * FROM `{project_id}.marts.pipeline_status`")
    return snapshot, history, health, pipeline


st.title("🇰🇪 Kenya Economic Intelligence")
st.caption("Official-source economic data · revision-aware · autonomously refreshed")

try:
    snapshot, history, health, pipeline = load_dashboard_data()
except Exception as exc:
    st.error("The warehouse is not ready or the dashboard cannot reach BigQuery.")
    st.exception(exc)
    st.stop()

usd_kes = indicator_value(snapshot, "USD_KES")
inflation = indicator_value(snapshot, "CPI_INFLATION_YOY")
gdp_growth = indicator_value(snapshot, "REAL_GDP_GROWTH")
gdp_lcu = indicator_value(snapshot, "GDP_CURRENT_LCU")

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("KES / USD", f"{usd_kes:.2f}" if usd_kes is not None else "—")
with c2:
    st.metric("Headline inflation", f"{inflation:.1f}%" if inflation is not None else "—")
with c3:
    st.metric("Real GDP growth", f"{gdp_growth:.1f}%" if gdp_growth is not None else "—")
with c4:
    st.metric("GDP · current KES", f"KES {gdp_lcu / 1e12:.2f}T" if gdp_lcu is not None else "—")

st.divider()

left, right = st.columns(2)
with left:
    st.subheader("Real GDP growth")
    gdp_history = history[history["indicator_code"] == "REAL_GDP_GROWTH"].copy()
    if gdp_history.empty:
        st.info("No GDP growth history is available yet.")
    else:
        fig = px.line(gdp_history, x="period_end", y="value", markers=True, labels={"period_end": "Year", "value": "%"})
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Headline inflation")
    cpi_history = history[history["indicator_code"] == "CPI_INFLATION_YOY"].copy()
    if cpi_history.empty:
        st.info("No KNBS inflation history is available yet.")
    else:
        fig = px.line(cpi_history, x="period_end", y="value", markers=True, labels={"period_end": "Month", "value": "%"})
        st.plotly_chart(fig, use_container_width=True)

st.divider()
st.subheader("Data health")

if health.empty:
    st.info("No source-health records yet.")
else:
    health_display = health.copy()
    health_display["Status"] = health_display["freshness_status"].map(
        lambda value: f"{status_icon(value)} {value.title()}"
    )
    columns = ["source", "Status", "latest_observation_date", "last_checked_at", "last_error"]
    columns = [column for column in columns if column in health_display.columns]
    st.dataframe(
        health_display[columns].rename(columns={
            "source": "Source",
            "latest_observation_date": "Latest observation",
            "last_checked_at": "Last checked",
            "last_error": "Last error",
        }),
        use_container_width=True,
        hide_index=True,
    )

if not pipeline.empty:
    latest_run = pipeline.iloc[0]
    completed = pd.to_datetime(latest_run.get("completed_at"), utc=True, errors="coerce")
    completed_text = completed.strftime("%d %b %Y · %H:%M UTC") if pd.notna(completed) else "unknown"
    st.caption(
        f"Pipeline: {str(latest_run.get('status', 'unknown')).upper()} · "
        f"last completed {completed_text} · rows added {int(latest_run.get('rows_inserted', 0) or 0)}"
    )

st.divider()
st.subheader("Current observations & provenance")
if snapshot.empty:
    st.info("No published observations yet.")
else:
    show = snapshot[[
        "source", "indicator_name", "observation_date", "value", "unit", "source_url", "ingested_at"
    ]].copy()
    st.dataframe(show, use_container_width=True, hide_index=True, column_config={"source_url": st.column_config.LinkColumn("Source")})
