from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.data import (
    annual_indicator_history,
    build_bigquery_client,
    build_economy_summary,
    display_source_name,
    format_eat_timestamp,
    has_enough_history,
    indicator_context,
    indicator_value,
    metric_change_label,
    prepare_health_for_display,
    prepare_snapshot_for_display,
    query_dataframe,
    recent_indicator_history,
    status_icon,
)


st.set_page_config(
    page_title="Kenya Economic Intelligence",
    page_icon="🇰🇪",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
        .block-container {
            max-width: 1450px;
            padding-top: 2.5rem;
            padding-bottom: 4rem;
        }

        [data-testid="stSidebar"] {
            border-right: 1px solid rgba(128, 128, 128, 0.18);
        }

        .eyebrow {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            opacity: 0.6;
            line-height: 1.4;
            padding-top: 0.15rem;
            padding-bottom: 0.15rem;
            margin-bottom: 0.35rem;
            display: block;
        }

        .hero-title {
            font-size: 2.35rem;
            font-weight: 760;
            letter-spacing: -0.045em;
            line-height: 1.05;
            margin-bottom: 0.35rem;
        }

        .hero-copy {
            font-size: 1.07rem;
            line-height: 1.75;
            opacity: 0.84;
            max-width: 1050px;
            margin-top: 0.5rem;
            margin-bottom: 1.5rem;
        }

        .section-kicker {
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.11em;
            text-transform: uppercase;
            opacity: 0.55;
            margin-bottom: 0.15rem;
        }

        .section-title {
            font-size: 1.55rem;
            font-weight: 720;
            letter-spacing: -0.025em;
            margin-bottom: 0.25rem;
        }

        .section-copy {
            opacity: 0.65;
            margin-bottom: 1.2rem;
        }

        .health-good {
            padding: 0.75rem 0.9rem;
            border: 1px solid rgba(46, 160, 67, 0.35);
            border-radius: 0.8rem;
            background: rgba(46, 160, 67, 0.08);
            margin: 0.65rem 0 1rem 0;
        }

        .health-bad {
            padding: 0.75rem 0.9rem;
            border: 1px solid rgba(248, 81, 73, 0.35);
            border-radius: 0.8rem;
            background: rgba(248, 81, 73, 0.08);
            margin: 0.65rem 0 1rem 0;
        }

        .sidebar-source {
            padding: 0.65rem 0;
            border-bottom: 1px solid rgba(128, 128, 128, 0.14);
        }

        .sidebar-source:last-child {
            border-bottom: none;
        }

        div[data-testid="stMetric"] {
            padding: 1rem 1.05rem;
            border: 1px solid rgba(128, 128, 128, 0.18);
            border-radius: 0.85rem;
            background: rgba(128, 128, 128, 0.035);
        }

        div[data-testid="stMetricLabel"] {
            opacity: 0.68;
        }

        [data-testid="stSidebar"] div[data-testid="stMetric"] {
            padding: 0.65rem 0.75rem;
            min-height: 0;
        }

        [data-testid="stSidebar"] div[data-testid="stMetricValue"] {
            font-size: 1.7rem;
        }

        .history-building {
            padding: 1rem 1.1rem;
            border: 1px solid rgba(128, 128, 128, 0.18);
            border-radius: 0.8rem;
            background: rgba(128, 128, 128, 0.04);
            color: rgba(250, 250, 250, 0.72);
            line-height: 1.6;
            margin-top: 0.7rem;
        }

        .metric-context {
            font-size: 0.78rem;
            opacity: 0.68;
            margin-top: -0.25rem;
            margin-bottom: 0.15rem;
        }

        hr {
            margin-top: 2.2rem;
            margin-bottom: 2.2rem;
            opacity: 0.15;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


def _secret(name: str, default=None):
    try:
        return st.secrets.get(name, default)
    except Exception:
        return default


@st.cache_resource
def get_client():
    project_id = _secret("GCP_PROJECT_ID") or os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError(
            "Set GCP_PROJECT_ID in Streamlit secrets or the environment"
        )

    service_account_info = _secret("gcp_service_account")
    if service_account_info:
        service_account_info = dict(service_account_info)

    return (
        build_bigquery_client(project_id, service_account_info),
        project_id,
    )


@st.cache_data(ttl=300)
def load_dashboard_data():
    client, project_id = get_client()

    snapshot = query_dataframe(
        client,
        f"SELECT * FROM `{project_id}.marts.economic_snapshot`",
    )

    history = query_dataframe(
        client,
        f"""
        SELECT *
        FROM `{project_id}.marts.indicator_history`
        ORDER BY period_end
        """,
    )

    health = query_dataframe(
        client,
        f"""
        SELECT *
        FROM `{project_id}.marts.source_health`
        ORDER BY source
        """,
    )

    pipeline = query_dataframe(
        client,
        f"""
        SELECT *
        FROM `{project_id}.marts.pipeline_status`
        """,
    )

    return snapshot, history, health, pipeline


def chart_frame(
    frame: pd.DataFrame,
    indicator_code: str,
    points: int,
) -> pd.DataFrame:
    result = recent_indicator_history(
        frame,
        indicator_code,
        points=points,
    ).copy()

    if "value" in result.columns:
        result["value"] = pd.to_numeric(
            result["value"],
            errors="coerce",
        )

    return result


def period_label(value) -> str:
    timestamp = pd.to_datetime(value, errors="coerce")

    if pd.isna(timestamp):
        return "Latest available"

    return timestamp.strftime("%b %Y")


try:
    snapshot, history, health, pipeline = load_dashboard_data()
except Exception as exc:
    st.error(
        "The warehouse is not ready or the dashboard cannot reach BigQuery."
    )
    st.exception(exc)
    st.stop()


# -------------------------------------------------------------------
# Context
# -------------------------------------------------------------------

usd_kes = indicator_value(snapshot, "USD_KES")
inflation = indicator_value(snapshot, "CPI_INFLATION_YOY")
gdp_growth = indicator_value(snapshot, "REAL_GDP_GROWTH")
gdp_lcu = indicator_value(snapshot, "GDP_CURRENT_LCU")

inflation_context = indicator_context(
    history,
    "CPI_INFLATION_YOY",
)

fx_context = indicator_context(
    history,
    "USD_KES",
)

gdp_context = indicator_context(
    history,
    "REAL_GDP_GROWTH",
)

summary = build_economy_summary(
    snapshot,
    history,
)


# -------------------------------------------------------------------
# Sidebar — trust layer
# -------------------------------------------------------------------

with st.sidebar:
    st.markdown("## 🇰🇪 Kenya Econ")
    st.caption("Economic intelligence · official sources")

    st.divider()

    st.markdown("### Pipeline health")

    if pipeline.empty:
        st.warning("No completed pipeline run found.")
    else:
        latest_run = pipeline.iloc[0]
        run_status = str(
            latest_run.get("status", "unknown")
        ).lower()

        completed_text = (
            format_eat_timestamp(
                latest_run.get("completed_at")
            )
            or "Unknown"
        )

        succeeded = int(
            latest_run.get("sources_succeeded", 0) or 0
        )
        failed = int(
            latest_run.get("sources_failed", 0) or 0
        )
        inserted = int(
            latest_run.get("rows_inserted", 0) or 0
        )

        if run_status == "success":
            st.markdown(
                """
                <div class="health-good">
                    <strong>● Healthy</strong><br>
                    <span style="opacity:.7">
                    Latest refresh completed successfully
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f"""
                <div class="health-bad">
                    <strong>▲ {run_status.title()}</strong><br>
                    <span style="opacity:.7">
                    Latest refresh needs attention
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

        p1, p2 = st.columns(2)

        with p1:
            st.metric(
                "Sources",
                f"{succeeded}/{succeeded + failed}",
            )

        with p2:
            st.metric(
                "Rows added",
                inserted,
            )

        st.caption(f"Last run · {completed_text}")

    st.caption("Schedule · Daily at 06:15 EAT")

    st.divider()

    st.markdown("### Data freshness")
    st.caption(
        "Each source has its own expected publishing cadence."
    )

    if health.empty:
        st.info("No source-health records available.")
    else:
        health_display = prepare_health_for_display(
            health
        )

        for _, row in health_display.iterrows():
            freshness = str(
                row.get("freshness_status", "UNKNOWN")
            )
            source_name = row.get("source", "Unknown")
            latest_date = pd.to_datetime(
                row.get("latest_observation_date"),
                errors="coerce",
            )

            latest_text = (
                latest_date.strftime("%d %b %Y")
                if pd.notna(latest_date)
                else "Unknown"
            )

            icon = status_icon(freshness)

            st.markdown(
                f"""
                <div class="sidebar-source">
                    <strong>{icon} {source_name}</strong><br>
                    <span style="opacity:.65">
                        {freshness.title()} · latest {latest_text}
                    </span>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.divider()

    st.caption(
        "Sources: Central Bank of Kenya · "
        "Kenya National Bureau of Statistics · World Bank"
    )


# -------------------------------------------------------------------
# Hero
# -------------------------------------------------------------------

st.markdown(
    '<div class="eyebrow">Kenya economic monitor</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="hero-title">Kenya Economic Intelligence</div>',
    unsafe_allow_html=True,
)

st.caption(
    "Official-source economic data · revision-aware · autonomously refreshed"
)

st.divider()

st.markdown(
    '<div class="section-kicker">Executive view</div>',
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='section-title'>Kenya's economy at a glance</div>",
    unsafe_allow_html=True,
)

st.markdown(
    f"<div class='hero-copy'>{summary}</div>",
    unsafe_allow_html=True,
)


# -------------------------------------------------------------------
# KPI layer
# -------------------------------------------------------------------

k1, k2, k3, k4 = st.columns(4)

with k1:
    st.metric(
        "KES / USD",
        f"{usd_kes:.2f}" if usd_kes is not None else "—",
        help="Kenya shillings per US dollar · Central Bank of Kenya",
    )
    fx_change_label = metric_change_label(
        "USD_KES",
        fx_context.get("change"),
    )

    if fx_change_label:
        st.markdown(
            f'<div class="metric-context">{fx_change_label} vs previous observation</div>',
            unsafe_allow_html=True,
        )

    st.caption(
        f"CBK · {period_label(fx_context.get('period_end'))}"
    )

with k2:
    st.metric(
        "Headline inflation",
        f"{inflation:.1f}%" if inflation is not None else "—",
        help="Year-on-year headline CPI inflation · KNBS",
    )
    inflation_change_label = metric_change_label(
        "CPI_INFLATION_YOY",
        inflation_context.get("change"),
    )

    if inflation_change_label:
        st.markdown(
            f'<div class="metric-context">{inflation_change_label} vs previous observation</div>',
            unsafe_allow_html=True,
        )

    st.caption(
        f"KNBS · {period_label(inflation_context.get('period_end'))}"
    )

with k3:
    st.metric(
        "Real GDP growth",
        f"{gdp_growth:.1f}%" if gdp_growth is not None else "—",
        help="Annual real GDP growth · World Bank",
    )
    gdp_change_label = metric_change_label(
        "REAL_GDP_GROWTH",
        gdp_context.get("change"),
    )

    if gdp_change_label:
        st.markdown(
            f'<div class="metric-context">{gdp_change_label} vs previous year</div>',
            unsafe_allow_html=True,
        )

    st.caption(
        f"World Bank · {period_label(gdp_context.get('period_end'))}"
    )

with k4:
    st.metric(
        "Nominal GDP",
        (
            f"KES {gdp_lcu / 1e12:.2f}T"
            if gdp_lcu is not None
            else "—"
        ),
        help="GDP in current local currency · World Bank",
    )

    gdp_rows = snapshot[
        snapshot["indicator_code"] == "GDP_CURRENT_LCU"
    ]

    if not gdp_rows.empty:
        gdp_period = gdp_rows.iloc[0].get(
            "observation_date"
        )
        st.caption(
            f"World Bank · {period_label(gdp_period)}"
        )


# -------------------------------------------------------------------
# What's changing now?
# -------------------------------------------------------------------

st.divider()

st.markdown(
    '<div class="section-kicker">Near-term signals</div>',
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='section-title'>What's changing now?</div>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-copy">
    Focus on the indicators that update most frequently:
    consumer prices and the Kenya shilling.
    </div>
    """,
    unsafe_allow_html=True,
)

left, right = st.columns(2)

with left:
    st.markdown("#### Inflation momentum")

    inflation_history = chart_frame(
        history,
        "CPI_INFLATION_YOY",
        points=12,
    )

    if inflation_history.empty:
        st.info("No recent inflation history available.")
    else:
        fig = px.line(
            inflation_history,
            x="period_end",
            y="value",
            markers=True,
            labels={
                "period_end": "",
                "value": "Inflation (%)",
            },
        )

        fig.update_layout(
            margin=dict(l=10, r=10, t=20, b=10),
            height=350,
            hovermode="x unified",
            showlegend=False,
        )

        st.plotly_chart(
            fig,
            width="stretch",
        )

        if inflation_context["change"] is not None:
            direction = (
                "higher"
                if inflation_context["change"] > 0
                else "lower"
                if inflation_context["change"] < 0
                else "unchanged"
            )

            previous_period = period_label(
                inflation_context.get("previous_period_end")
            )

            st.caption(
                f"Latest: {inflation_context['current']:.1f}% · "
                f"{direction} by "
                f"{abs(inflation_context['change']):.1f} pp "
                f"from {previous_period}."
            )


with right:
    st.markdown("#### Shilling movement")

    fx_history = chart_frame(
        history,
        "USD_KES",
        points=30,
    )

    if not has_enough_history(
        history,
        "USD_KES",
        minimum_points=7,
    ):
        st.markdown(
            """
            <div class="history-building">
                <strong>History still building</strong><br>
                Daily CBK observations are accumulating.
                The exchange-rate trend will appear automatically
                once at least 7 observations are available.
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        fig = px.line(
            fx_history,
            x="period_end",
            y="value",
            markers=True,
            labels={
                "period_end": "",
                "value": "KES per USD",
            },
        )

        fig.update_layout(
            margin=dict(l=10, r=10, t=20, b=10),
            height=350,
            hovermode="x unified",
            showlegend=False,
        )

        st.plotly_chart(
            fig,
            width="stretch",
        )

        if fx_context["change"] is not None:
            if fx_context["change"] > 0:
                interpretation = (
                    "The shilling is weaker against the dollar "
                    "than the previous observation."
                )
            elif fx_context["change"] < 0:
                interpretation = (
                    "The shilling is stronger against the dollar "
                    "than the previous observation."
                )
            else:
                interpretation = (
                    "The exchange rate is unchanged from "
                    "the previous observation."
                )

            st.caption(interpretation)


# -------------------------------------------------------------------
# Economic performance
# -------------------------------------------------------------------

st.divider()

st.markdown(
    '<div class="section-kicker">Structural view</div>',
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='section-title'>Economic performance</div>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-copy">
    Real GDP growth provides the longer-term context behind
    the faster-moving inflation and currency signals.
    </div>
    """,
    unsafe_allow_html=True,
)

gdp_history = annual_indicator_history(
    history,
    "REAL_GDP_GROWTH",
    points=15,
)

if gdp_history.empty:
    st.info("No GDP growth history is available.")
else:
    fig = px.bar(
        gdp_history,
        x="year",
        y="value",
        labels={
            "year": "",
            "value": "Real GDP growth (%)",
        },
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=20, b=10),
        height=410,
        showlegend=False,
    )

    st.plotly_chart(
        fig,
        width="stretch",
    )

    if gdp_context.get("change") is not None:
        gdp_direction = (
            "up"
            if gdp_context["change"] > 0
            else "down"
            if gdp_context["change"] < 0
            else "unchanged"
        )

        previous_year = period_label(
            gdp_context.get("previous_period_end")
        )

        st.caption(
            f"Latest: {gdp_context['current']:.1f}% · "
            f"{gdp_direction} "
            f"{abs(gdp_context['change']):.1f} pp "
            f"from {previous_year}. "
            f"Showing the latest 15 annual observations."
        )
    else:
        st.caption(
            "Showing the latest 15 annual observations. "
            "The complete historical series remains available "
            "in the warehouse."
        )


# -------------------------------------------------------------------
# Trust + methodology
# -------------------------------------------------------------------

st.divider()

st.markdown(
    '<div class="section-kicker">Trust & provenance</div>',
    unsafe_allow_html=True,
)

st.markdown(
    "<div class='section-title'>Sources & methodology</div>",
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="section-copy">
    Every published indicator retains its source,
    observation date and ingestion timestamp.
    </div>
    """,
    unsafe_allow_html=True,
)

with st.expander(
    "View current observations and provenance",
    expanded=False,
):
    if snapshot.empty:
        st.info("No published observations yet.")
    else:
        show = snapshot[
            [
                "source",
                "indicator_name",
                "observation_date",
                "value",
                "unit",
                "source_url",
                "ingested_at",
            ]
        ].copy()

        show = prepare_snapshot_for_display(show)

        show = show.rename(
            columns={
                "source": "Source",
                "indicator_name": "Indicator",
                "observation_date": "Observation date",
                "value": "Value",
                "unit": "Unit",
                "source_url": "Official source",
                "ingested_at": "Ingested",
            }
        )

        st.dataframe(
            show,
            width="stretch",
            hide_index=True,
            column_config={
                "Official source": st.column_config.LinkColumn(
                    "Official source",
                    display_text="Open ↗",
                ),
            },
        )


with st.expander(
    "How to read this dashboard",
    expanded=False,
):
    st.markdown(
        """
        **Freshness is source-specific.** A daily exchange-rate
        observation and an annual GDP observation should not be
        judged by the same age threshold.

        **The raw warehouse is append-only and revision-aware.**
        Historical source revisions are preserved before dbt
        publishes the latest trusted observation.

        **Narrative text is deterministic.** The summary above is
        generated directly from warehouse values and comparison
        logic; it is not produced by a language model.

        **Primary sources**
        - Central Bank of Kenya — USD/KES exchange rate
        - Kenya National Bureau of Statistics — headline CPI inflation
        - World Bank — historical GDP and real GDP growth
        """
    )
