# 🇰🇪 Kenya Economic Intelligence Pipeline

A production-grade, end-to-end data engineering project that ingests, transforms, streams, and visualises Kenya's macroeconomic indicators in real time — built on the modern data stack.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.7-orange.svg)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.8-green.svg)](https://airflow.apache.org)
[![Kafka](https://img.shields.io/badge/Kafka-7.5-black.svg)](https://kafka.apache.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-red.svg)](https://streamlit.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://postgresql.org)

---

## Problem Statement

Kenya's economic data — GDP, inflation, and foreign exchange rates — is scattered across the World Bank, the Central Bank of Kenya, and third-party FX providers with no unified, automated data layer. Analysts manually download CSVs, reconcile different formats, and build one-off spreadsheets. Fintech and investment teams lack a reliable, always-fresh FX feed. Policy teams cannot track macro trends in near real-time.

This pipeline solves that. It automates the full data journey — from raw API ingestion to a live dashboard — and produces analytical outputs no single source can provide, including Kenya's GDP expressed in KES, a real purchasing power index, and a continuously updating KES/USD time-series.

---

## Live Dashboard

🔗 **[kenya-economic-intelligence.streamlit.app](https://gishusam-kenya-econ-pipeline-dashboardapp-olrpi8.streamlit.app)**

---

## What It Produces

| Metric | Description | Source |
|--------|-------------|--------|
| GDP in KES | Kenya GDP converted from USD at live FX rate | World Bank × Open Exchange Rates |
| YoY GDP growth | Year-over-year percentage change | Derived by dbt |
| Inflation trend | Annual CPI rate 2020–2024 | World Bank |
| Purchasing power index | Inflation + FX depreciation combined | Flagship dbt model |
| Live KES/USD rate | Streamed every 60 seconds via Kafka | Open Exchange Rates |
| FX daily summary | Min, max, average rate per day | Derived by dbt |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                             │
│   World Bank API          Open Exchange Rates API               │
│   (GDP, inflation)        (KES/USD live rate)                   │
└──────────┬───────────────────────────┬──────────────────────────┘
           │                           │
           ▼                           ▼
┌──────────────────┐        ┌─────────────────────┐
│  Python Ingestion │        │   Kafka Producer     │
│  worldbank.py     │        │   fx_producer.py     │
│  fx_rates.py      │        │   (every 60 seconds) │
└──────────┬────────┘        └──────────┬──────────┘
           │                            │
           ▼                            ▼
┌──────────────────┐        ┌─────────────────────┐
│  data/raw/       │        │   Kafka Topic        │
│  JSON files      │        │   fx_rates_ke        │
└──────────┬────────┘        └──────────┬──────────┘
           │                            │
           ▼                            ▼
┌──────────────────────────────────────────────────┐
│              PostgreSQL Warehouse                 │
│  ┌──────────┐  ┌───────────┐  ┌───────────────┐  │
│  │   raw    │→ │  staging  │→ │     mart      │  │
│  │ (exact)  │  │ (cleaned) │  │  (analytics)  │  │
│  └──────────┘  └───────────┘  └───────────────┘  │
└──────────────────────┬───────────────────────────┘
                       │
           ┌───────────┼───────────┐
           ▼           ▼           ▼
      dbt models   Airflow DAG  Streamlit
      (transform)  (schedule)   (dashboard)
```

---

## Tech Stack

| Layer | Tool | Version |
|-------|------|---------|
| Ingestion | Python, requests, tenacity | 3.11+ |
| Storage | PostgreSQL | 15 |
| Transformation | dbt-postgres | 1.7 |
| Orchestration | Apache Airflow | 2.8 |
| Streaming | Apache Kafka (Confluent) | 7.5 |
| Streaming client | kafka-python | 2.0.2 |
| Dashboard | Streamlit, Plotly | 1.32, 5.20 |
| Infrastructure | Docker, Docker Compose | — |
| Production DB | Neon (serverless Postgres) | — |

---

## Project Structure

```
kenya-econ-pipeline/
│
├── ingestion/                  # API clients
│   ├── base_client.py          # Shared HTTP logic, retry, logging
│   ├── worldbank.py            # World Bank GDP + inflation ingester
│   └── fx_rates.py             # Open Exchange Rates FX ingester
│
├── db/                         # Storage layer
│   ├── connection.py           # Connection manager (context manager)
│   ├── migrate.py              # Runs versioned SQL migrations
│   ├── migrations/
│   │   ├── 001_create_schemas.sql
│   │   ├── 002_create_raw_tables.sql
│   │   └── 003_create_staging_tables.sql
│   └── loaders/
│       ├── worldbank_loader.py # JSON → raw → staging
│       └── fx_loader.py        # JSON → raw → staging
│
├── kenya_econ_dbt/             # Transformation layer
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_gdp.sql
│   │   │   ├── stg_inflation.sql
│   │   │   └── stg_fx.sql
│   │   └── mart/
│   │       ├── kenya_macro.sql
│   │       ├── fx_daily_summary.sql
│   │       └── purchasing_power.sql
│   └── macros/
│       └── generate_schema_name.sql
│
├── dags/                       # Orchestration layer
│   └── kenya_econ_dag.py       # Airflow DAG (6 tasks, daily 06:00 EAT)
│
├── streaming/                  # Streaming layer
│   ├── topic_setup.py          # Creates Kafka topics
│   ├── fx_producer.py          # Publishes FX events every 60s
│   └── fx_consumer.py          # Writes Kafka events to Postgres
│
├── dashboard/                  # Visualisation layer
│   ├── app.py                  # Streamlit dashboard
│   └── requirements.txt        # Dashboard-only dependencies
│
├── utils/                      # Shared utilities
│   ├── logger.py               # JSON structured logging
│   └── validators.py           # API response validation
│
├── docker-compose.yml          # Full stack (Postgres, Airflow, Kafka)
├── Makefile                    # One-command setup
├── requirements.txt            # All dependencies
└── .env.example                # Environment variable template
```

---

## Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- A free API key from [openexchangerates.org](https://openexchangerates.org)

### Setup

```bash
# 1. Clone the repository
git clone https://github.com/samwelngugi/kenya-econ-pipeline.git
cd kenya-econ-pipeline

# 2. Copy environment template and add your API key
cp .env.example .env

# 3. Spin up infrastructure and load data
make setup

# 4. Launch the dashboard
make dashboard
# Opens at http://localhost:8501
```

### Manual setup (step by step)

```bash
# Start Docker services
docker-compose up postgres zookeeper kafka -d

# Run database migrations
python -m db.migrate

# Ingest data from APIs
python -m ingestion.worldbank
python -m ingestion.fx_rates

# Load into Postgres
python -m db.loaders.worldbank_loader
python -m db.loaders.fx_loader

# Run dbt transformations
cd kenya_econ_dbt && dbt run && dbt test

# Start Kafka streaming (two terminals)
python -m streaming.fx_producer    # Terminal 1
python -m streaming.fx_consumer    # Terminal 2

# Launch dashboard
streamlit run dashboard/app.py
```

---

## Data Sources

| Source | Data | Auth | Cost |
|--------|------|------|------|
| [World Bank API](https://datahelpdesk.worldbank.org/knowledgebase/articles/889392) | GDP, CPI inflation for Kenya | None required | Free |
| [Open Exchange Rates](https://openexchangerates.org) | Live KES/USD, EUR, GBP rates | Free API key | Free tier |

---

## Pipeline Details

### Airflow DAG — `kenya_econ_pipeline`

Runs daily at **06:00 EAT (03:00 UTC)** with the following task chain:

```
migrate_db → [worldbank_ingest, fx_ingest] → load_to_postgres → dbt_run → dbt_test
```

| Task | Type | Description |
|------|------|-------------|
| `migrate_db` | PythonOperator | Creates schemas and tables if not present |
| `worldbank_ingest` | PythonOperator | Pulls GDP + inflation from World Bank API |
| `fx_ingest` | PythonOperator | Pulls KES/USD rate from Open Exchange Rates |
| `load_to_postgres` | PythonOperator | Loads raw JSON → raw schema → staging schema |
| `dbt_run` | BashOperator | Builds all mart models |
| `dbt_test` | BashOperator | Runs 14 data quality tests |

### dbt Models

| Model | Schema | Type | Description |
|-------|--------|------|-------------|
| `stg_gdp` | staging | View | Cleaned World Bank GDP data |
| `stg_inflation` | staging | View | Cleaned CPI inflation data |
| `stg_fx` | staging | View | Enriched FX rates with derived columns |
| `kenya_macro` | mart | Table | Core macro table — GDP, inflation, FX, growth |
| `fx_daily_summary` | mart | Table | Daily min/max/avg KES rate |
| `purchasing_power` | mart | Table | Flagship metric — inflation × FX index |

### Kafka Streaming

```
Producer (fx_producer.py)
  └── Polls Open Exchange Rates every 60 seconds
  └── Publishes JSON event to topic: fx_rates_ke
        └── { base, target_currency, rate, event_timestamp }

Consumer (fx_consumer.py)
  └── Reads from topic fx_rates_ke
  └── Derives usd_per_kes = 1 / rate
  └── Writes to staging.stg_fx_rates (ON CONFLICT DO NOTHING)
```

### Three-Layer Warehouse

| Layer | Schema | Purpose |
|-------|--------|---------|
| Raw | `raw` | Exact copy of API response — audit trail, never modified |
| Staging | `staging` | Typed, cleaned, deduplicated — SCD Type 2 on World Bank data |
| Mart | `mart` | Business logic, derived metrics, analytics-ready |

---

## Key Engineering Decisions

**Idempotency** — Every loader uses `ON CONFLICT DO NOTHING`. Every dbt model is a `CREATE OR REPLACE`. The pipeline can run any number of times and produce the same result.

**SCD Type 2** — Staging tables for World Bank data include `valid_from`, `valid_to`, and `is_current` columns. When the World Bank revises a historical GDP figure, the old row closes and a new one opens — full revision history preserved.

**Separation of concerns** — Raw data is never modified after landing. Staging imposes data types and structure. Mart imposes business logic. A failure in any layer never corrupts another.

**Structured logging** — Every module uses JSON-formatted logs compatible with Airflow's log aggregation. No `print()` statements anywhere in the codebase.

**Connection resilience** — SQLAlchemy engine uses `pool_pre_ping=True` and `pool_recycle=300` to handle stale connections. Postgres connections include `idle_in_transaction_session_timeout` to prevent abandoned Airflow tasks from holding locks.

---

## Dashboard

The Streamlit dashboard connects directly to the mart schema and displays:

- **KPI row** — Latest GDP (USD + KES), inflation rate, live KES/USD from Kafka
- **GDP bar chart** — Annual GDP 2020–2024 in USD billions
- **Inflation line chart** — CPI trend showing the 2022–2023 peak and 2024 recovery
- **KES/USD time-series** — Live stream from Kafka, every hourly snapshot
- **Purchasing power index** — Compound effect of inflation and FX depreciation
- **YoY growth chart** — Colour-coded positive/negative growth by year
- **Data tables** — Full macro summary and live FX feed

---

## What the Data Shows

Kenya's GDP contracted in USD terms from $114.4B (2022) to $107.5B (2023) — not because the economy shrank, but because KES depreciated significantly against the dollar. In local currency, the economy continued growing. By 2024, GDP recovered strongly to $120.3B (+11.94% YoY) while inflation cooled from 7.67% to 4.49%.

The purchasing power index — a metric this pipeline derives that no single data source provides — shows that despite GDP growth, Kenyan households have never regained the purchasing power of 2020 (index peaked at 95.70 in 2024 vs baseline 100).

---

## Project Status

- [x] Week 1 — Ingestion layer (World Bank + FX APIs, retry logic, structured logging)
- [x] Week 2 — Storage layer (Docker, PostgreSQL, 3-layer schema, SCD Type 2)
- [x] Week 3 — Transformation layer (dbt models, 14 data quality tests, lineage graph)
- [x] Week 4 — Orchestration layer (Airflow DAG, 6 tasks, daily schedule, retry logic)
- [x] Week 5 — Streaming layer (Kafka producer/consumer, real-time KES/USD feed)
- [x] Week 6 — Dashboard + deploy (Streamlit, Plotly, Neon, Streamlit Cloud)

---

## Author

**Samwel Ngugi** — Data Engineer, Nairobi, Kenya

Building data pipelines that solve real problems in African markets.

[GitHub](https://github.com/gishusam) · [LinkedIn](https://www.linkedin.com/in/samwelngugi/)

---


