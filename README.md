# 🇰🇪 Kenya Economic Intelligence

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-Open-FF4B4B?logo=streamlit&logoColor=white)](https://kenya-economic-intelligence.streamlit.app/)
[![CI](https://github.com/gishusam/kenya-econ-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/gishusam/kenya-econ-pipeline/actions/workflows/ci.yml)

An autonomous, revision-aware data pipeline for Kenya's core economic indicators.

**Live dashboard:** https://kenya-economic-intelligence.streamlit.app/

The project collects official economic observations from **KNBS**, **CBK**, and the **World Bank**, preserves source revisions in BigQuery, standardizes them with dbt, and publishes trusted marts to a Streamlit dashboard. The production path is intentionally batch-oriented: **no Airflow, no Kafka, and no always-on server**.

> **Architecture principle:** the system should keep collecting, validating, and publishing data when the developer's laptop is switched off. Human intervention is reserved for failures, source-format changes, and model changes.

## The real-world problem

Kenyan economic data is published by different institutions, on different cadences, and in different formats. A finance, research, strategy, or investment team tracking GDP, inflation, and exchange rates has to repeatedly:

- locate the latest official release;
- determine whether a historical observation was revised;
- reconcile incompatible source schemas;
- verify whether its local dataset is still current; and
- refresh downstream analysis without silently mixing stale and fresh data.

That is a **data reliability problem before it is an analytics problem**.

Kenya Economic Intelligence automates that operational layer. It checks authoritative sources, appends new and revised observations without overwriting history, applies a canonical data contract, runs dbt tests, and exposes freshness-aware analytical tables to the dashboard.

## Architecture

```text
                         Google Cloud Scheduler
                         europe-west1 (06:15 EAT)
                                  │
                                  ▼
                         Cloud Run Job
                      kenya-econ-refresh
                       africa-south1
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
               KNBS              CBK          World Bank
                 │                │                │
                 └────────────────┼────────────────┘
                                  │
                    canonicalize + fingerprint
                                  │
                                  ▼
                           BigQuery · raw
                    append-only source revisions
                                  │
                               dbt build
                                  │
                                  ▼
                        BigQuery · staging
                       canonical data contract
                                  │
                              dbt tests
                                  │
                                  ▼
                          BigQuery · marts
                     trusted/latest observations
                                  │
                                  ▼
                         Streamlit Cloud
                    economics + data health

GitHub ── PR ──> CI tests / dbt parse / container build
   │
   └── main ──> WIF auth ──> Artifact Registry ──> Cloud Run deploy
```

`africa-south1` keeps BigQuery and Cloud Run together in Johannesburg. Cloud Scheduler is configured separately in `europe-west1` because Scheduler is not available in Johannesburg.

## Why these tools

| Concern | Choice | Why |
|---|---|---|
| Warehouse | BigQuery | Serverless warehouse; no database server to patch or keep running |
| Transformations | dbt-bigquery | Contracts, tests, lineage, and explicit transformation ownership |
| Runtime | Cloud Run Jobs | Bounded batch execution with no always-on compute |
| Scheduling | Cloud Scheduler | Independent of GitHub repository activity and developer machines |
| CI/CD | GitHub Actions | One place for tests and deployment; WIF avoids long-lived GCP keys |
| Dashboard | Streamlit Community Cloud | Lightweight public data-product UI with no local runtime |

### Why not Airflow?

The pipeline is a small linear batch workflow. Running an Airflow scheduler/webserver 24/7 would create more infrastructure than orchestration value. Airflow is better demonstrated in a project with genuine task interdependency.

### Why not Kafka?

These sources are low-volume and batch-oriented. There is no high-throughput event stream to buffer and no fan-out of independent consumers. An earlier version used Kafka for polled FX data; removing it is deliberate simplification, not a missing feature.

## Data sources

The first production slice ingests:

| Source | Indicator | Frequency | Role |
|---|---|---:|---|
| KNBS | Headline CPI inflation (YoY) | Monthly | Authoritative Kenya inflation release |
| CBK | USD/KES exchange rate | Daily | Authoritative domestic FX observation |
| World Bank | Real GDP growth | Annual | Historical macro series |
| World Bank | GDP in current local currency | Annual | Historical nominal GDP series in KES |

The daily pipeline *checks* all sources every run, but unchanged observations are not re-appended.

## Warehouse design

The warehouse uses three analytical layers plus an operational metadata dataset.

```text
raw  →  staging  →  marts
          │
metadata ─┴──── pipeline/source health
```

### `raw`

`raw.economic_observations` is append-only. Every ingested row carries:

- the source and indicator identity;
- observation period and value;
- source URL and original parsed payload;
- deterministic `source_record_hash`;
- `run_id` and ingestion timestamp.

If an institution revises a value, the old row remains and a new revision is appended.

### `staging`

`staging.stg_economic_observations` gives every source the same contract and ranks revisions for each natural observation key.

Natural revision key:

```text
(source, indicator_code, geography, period_start, period_end)
```

### `marts`

Dashboard-facing models use only the latest known revision per period.

- `marts.indicator_history` — historical series with revisions resolved.
- `marts.latest_indicators` — latest observation per source/indicator.
- `marts.economic_snapshot` — compact dashboard snapshot with provenance.
- `marts.source_health` — last source check + observation freshness.
- `marts.pipeline_status` — latest completed pipeline execution.

The Streamlit application queries **marts only**.

## Failure semantics

Source checks are isolated. A temporary failure in one source does not discard successful ingestion from the others.

| Outcome | Pipeline state |
|---|---|
| All sources + dbt succeed | `success` |
| One or more sources fail but dbt builds from existing valid data | `degraded` |
| dbt fails, or every source fails | `failed` |

A degraded run exits non-zero in Cloud Run intentionally: the valid data remains available, while the platform still surfaces that intervention may be required.

**Rule:** stale data may remain visible, but it must never be presented as fresh.

## Project structure

```text
pipeline/
  models.py              canonical observation contract
  hashing.py             deterministic revision fingerprints
  refresh.py             source-isolated refresh orchestration
  warehouse.py           append-only BigQuery adapter
  sources/
    knbs.py
    cbk.py
    world_bank.py

kenya_econ_dbt/
  models/
    staging/
    marts/
  profiles.yml

dashboard/
  app.py
  data.py

infra/
  bigquery/bootstrap.sql
  gcp/bootstrap.sh

.github/workflows/
  ci.yml
  deploy.yml

tests/
```

## Local development

Python 3.11 is the production runtime.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

Compile check:

```bash
make compile
```

To run the dashboard locally, install its UI dependencies separately:

```bash
pip install -r dashboard/requirements.txt
streamlit run streamlit_app.py
```

With Google Application Default Credentials and a GCP project configured:

```bash
cp .env.example .env
export GCP_PROJECT_ID="your-project-id"
export BQ_LOCATION="africa-south1"

dbt parse \
  --project-dir kenya_econ_dbt \
  --profiles-dir kenya_econ_dbt \
  --target prod
```

## First GCP deployment

The bootstrap script creates the required APIs, BigQuery datasets/tables, Artifact Registry repository, service accounts, and GitHub Workload Identity Federation configuration.

```bash
export PROJECT_ID="your-project-id"
export GITHUB_REPO="gishusam/kenya-econ-pipeline"
bash infra/gcp/bootstrap.sh
```

It prints the GitHub repository variables required by `.github/workflows/deploy.yml`, including separate compute and Scheduler regions.

After those variables are set, a push to `main` builds the container and deploys `kenya-econ-refresh`. The deploy workflow creates/updates the daily Cloud Scheduler trigger for **06:15 Africa/Nairobi**.

## Streamlit deployment

The dashboard needs:

```toml
GCP_PROJECT_ID = "your-project-id"
BQ_LOCATION = "africa-south1"
```

Streamlit Community Cloud must use the dedicated **read-only** dashboard service account if a credential is required. See `.streamlit/secrets.example.toml`. Do not reuse the Cloud Run or deployment identity.

## CI/CD security

GitHub Actions uses Google Workload Identity Federation. No long-lived Google service-account JSON key is stored in GitHub.

The identities are intentionally separate:

- `kenya-econ-pipeline` — Cloud Run ingestion/dbt runtime;
- `kenya-econ-scheduler` — permission to invoke the job;
- `kenya-econ-deploy` — GitHub deployment identity;
- `kenya-econ-dashboard` — read-only dashboard identity.

## Migration from v1

Version 1 demonstrated Postgres, Airflow, Kafka, dbt, and Streamlit, but several components existed primarily to demonstrate tooling rather than to serve this workload. Version 2 deliberately removes:

- local Postgres and database migrations/loaders;
- Docker Compose as the production runtime;
- Airflow DAG/webserver/scheduler configuration;
- Kafka, ZooKeeper, producer, and consumer processes;
- OpenExchangeRates from the authoritative data path;
- local raw JSON files as pipeline state.

Git history preserves the original implementation; dead runtime code is not retained in a `legacy/` directory.

## Current rebuild status

- [x] Canonical revision-aware observation contract
- [x] KNBS, CBK, and World Bank source adapters
- [x] Append-only BigQuery warehouse adapter
- [x] Source-isolated refresh runner
- [x] BigQuery dbt staging/mart models and contracts
- [x] Streamlit BigQuery/data-health migration
- [x] Cloud Run container definition
- [x] GitHub Actions CI/CD + WIF bootstrap
- [x] Apply migration to the GitHub repository
- [x] Bootstrap the target GCP project and execute the first live run
- [x] Configure Streamlit Community Cloud against the new marts

## Design docs

- `docs/superpowers/specs/2026-08-24-autonomous-kenya-econ-design.md`
- `docs/superpowers/plans/2026-08-24-autonomous-kenya-econ.md`
