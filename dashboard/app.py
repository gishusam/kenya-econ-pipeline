import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Kenya Economic Intelligence",
    page_icon="🇰🇪",
    layout="wide",
)

def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", 5432)
    db = os.getenv("POSTGRES_DB", "kenya_econ")
    return create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"connect_timeout": 10},
    )

@st.cache_data(ttl=300)
def load_macro():
    with get_engine().connect() as conn:
        return pd.read_sql("""
            SELECT year, gdp_usd_billions, gdp_kes_billions,
                   inflation_pct, gdp_growth_pct, kes_per_usd
            FROM mart.kenya_macro
            ORDER BY year DESC
        """, conn)

@st.cache_data(ttl=60)
def load_fx():
    with get_engine().connect() as conn:
        return pd.read_sql("""
            SELECT rate_date, rate_min, rate_max,
                   rate_avg, snapshot_count
            FROM mart.fx_daily_summary
            ORDER BY rate_date DESC
            LIMIT 30
        """, conn)

@st.cache_data(ttl=60)
def load_fx_live():
    with get_engine().connect() as conn:
        return pd.read_sql("""
            SELECT rate_timestamp, rate, usd_per_kes
            FROM staging.stg_fx_rates
            ORDER BY rate_timestamp DESC
            LIMIT 100
        """, conn)

@st.cache_data(ttl=300)
def load_purchasing_power():
    with get_engine().connect() as conn:
        return pd.read_sql("""
            SELECT year, inflation_pct, kes_per_usd,
                   inflation_index, fx_strength_index,
                   purchasing_power_index
            FROM mart.purchasing_power
            ORDER BY year ASC
        """, conn)

# ── Header ──────────────────────────────────────────────
st.title("🇰🇪 Kenya Economic Intelligence Dashboard")
st.caption("Real-time pipeline · World Bank · Open Exchange Rates · Kafka stream")

macro = load_macro()
fx_live = load_fx_live()
pp = load_purchasing_power()

latest = macro.iloc[0]
prev = macro.iloc[1] if len(macro) > 1 else None
latest_fx = fx_live.iloc[0] if len(fx_live) > 0 else None

# ── KPI Row ──────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    gdp_delta = (
        f"{latest['gdp_growth_pct']:+.1f}% YoY"
        if pd.notna(latest["gdp_growth_pct"])
        else "N/A"
    )
    st.metric("GDP (latest year)", f"${latest['gdp_usd_billions']:.1f}B", gdp_delta)

with col2:
    gdp_kes = latest["gdp_kes_billions"]
    st.metric("GDP in KES", f"KES {gdp_kes:,.0f}B", "at current FX rate")

with col3:
    inf = latest["inflation_pct"]
    inf_prev = prev["inflation_pct"] if prev is not None else None
    inf_delta = (
        f"{inf - inf_prev:+.2f}pp vs prior year"
        if inf_prev is not None and pd.notna(inf_prev)
        else ""
    )
    st.metric("Inflation rate", f"{inf:.2f}%", inf_delta)

with col4:
    if latest_fx is not None:
        rate = float(latest_fx["rate"])
        st.metric("KES / USD (live)", f"{rate:.2f}", "Kafka stream")
    else:
        st.metric("KES / USD", f"{latest['kes_per_usd']:.2f}", "batch")

st.divider()

# ── Charts Row 1 ──────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Kenya GDP — USD billions")
    macro_chart = macro.sort_values("year")
    fig = px.bar(
        macro_chart,
        x="year",
        y="gdp_usd_billions",
        color_discrete_sequence=["#1D9E75"],
        labels={"gdp_usd_billions": "GDP (USD B)", "year": "Year"},
        text="gdp_usd_billions",
    )
    fig.update_traces(texttemplate="%{text:.1f}B", textposition="outside")
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Inflation rate — annual %")
    macro_chart = macro.sort_values("year")
    fig = px.line(
        macro_chart,
        x="year",
        y="inflation_pct",
        color_discrete_sequence=["#D85A30"],
        markers=True,
        labels={"inflation_pct": "Inflation (%)", "year": "Year"},
    )
    fig.update_traces(marker=dict(size=8))
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Charts Row 2 ──────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("KES/USD — live Kafka stream")
    if len(fx_live) > 0:
        fx_chart = fx_live.sort_values("rate_timestamp")
        fig = px.line(
            fx_chart,
            x="rate_timestamp",
            y="rate",
            color_discrete_sequence=["#7F77DD"],
            labels={"rate": "KES per USD", "rate_timestamp": "Time"},
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0),
            yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No FX stream data yet — start the Kafka consumer")

with col2:
    st.subheader("Purchasing power index")
    fig = px.line(
        pp,
        x="year",
        y="purchasing_power_index",
        color_discrete_sequence=["#BA7517"],
        markers=True,
        labels={"purchasing_power_index": "Index", "year": "Year"},
    )
    fig.add_hline(
        y=100,
        line_dash="dash",
        line_color="gray",
        opacity=0.4,
        annotation_text="baseline 100",
    )
    fig.update_traces(marker=dict(size=8))
    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=10, b=0),
        yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── GDP Growth Bar ─────────────────────────────────────────
st.subheader("Year-over-year GDP growth %")
growth_data = macro.dropna(subset=["gdp_growth_pct"]).sort_values("year")
fig = px.bar(
    growth_data,
    x="year",
    y="gdp_growth_pct",
    color="gdp_growth_pct",
    color_continuous_scale=["#D85A30", "#1D9E75"],
    labels={"gdp_growth_pct": "Growth (%)", "year": "Year"},
    text="gdp_growth_pct",
)
fig.update_traces(texttemplate="%{text:+.2f}%", textposition="outside")
fig.update_xaxes(type="category", categoryorder="category ascending")
fig.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=10, b=0),
    showlegend=False,
    coloraxis_showscale=False,
    yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.1)"),
)
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Data Tables ──────────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("Macro summary")
    st.dataframe(
        macro[
            ["year", "gdp_usd_billions", "gdp_kes_billions",
             "inflation_pct", "gdp_growth_pct"]
        ].rename(columns={
            "year": "Year",
            "gdp_usd_billions": "GDP USD (B)",
            "gdp_kes_billions": "GDP KES (B)",
            "inflation_pct": "Inflation %",
            "gdp_growth_pct": "Growth %",
        }),
        use_container_width=True,
        hide_index=True,
    )

with col2:
    st.subheader("Live FX feed")
    if len(fx_live) > 0:
        st.dataframe(
            fx_live[["rate_timestamp", "rate", "usd_per_kes"]].rename(columns={
                "rate_timestamp": "Timestamp",
                "rate": "KES/USD",
                "usd_per_kes": "USD/KES",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("Start the Kafka consumer to see live data")

st.divider()

# ── Footer ───────────────────────────────────────────────
st.caption(
    "Pipeline: Airflow · dbt · Kafka · PostgreSQL · "
    "Data: World Bank + Open Exchange Rates · "
    "Built by Samwel Ngugi"
)