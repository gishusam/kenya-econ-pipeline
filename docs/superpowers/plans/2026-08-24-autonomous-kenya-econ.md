# Autonomous Kenya Economic Intelligence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the laptop-dependent Airflow/Kafka/Postgres runtime with a tested BigQuery/dbt/Cloud Run batch pipeline and deployable Streamlit data product.

**Architecture:** One Cloud Run Job checks all three official sources independently, appends only new/revised observations to BigQuery raw, then runs dbt to produce standardized staging models and trusted marts. Cloud Scheduler invokes the job; GitHub Actions owns CI/CD; Streamlit reads marts only.

**Tech Stack:** Python 3.11, requests, BeautifulSoup4, google-cloud-bigquery, dbt-bigquery, pytest, Streamlit, GitHub Actions, Cloud Run Jobs, Cloud Scheduler.

**Spec:** `docs/superpowers/specs/2026-08-24-autonomous-kenya-econ-design.md`

## Global Constraints
- No Airflow or Kafka in the production path.
- BigQuery is the production warehouse.
- Raw data is append-only and revision-preserving.
- Source failures are isolated and recorded; stale data is never silently labeled fresh.
- GitHub Actions uses Workload Identity Federation, never a long-lived GCP service-account JSON key.
- Streamlit queries marts only.

---

### Task 1: Canonical observation and deterministic revision hashing
**Files:**
- Create: `pipeline/models.py`
- Create: `pipeline/hashing.py`
- Test: `tests/test_observation.py`

**Interfaces:**
- Produces: `Observation`, `Observation.with_ingestion(run_id, ingested_at)`, `record_hash(observation)`.

- [ ] Write tests proving equivalent source observations hash identically and changed values hash differently.
- [ ] Run the tests and verify failure because production modules do not exist.
- [ ] Implement immutable observation model and canonical JSON hashing.
- [ ] Run tests and verify pass.

### Task 2: Official-source parsers
**Files:**
- Create: `pipeline/sources/base.py`
- Create: `pipeline/sources/knbs.py`
- Create: `pipeline/sources/cbk.py`
- Create: `pipeline/sources/world_bank.py`
- Create fixtures under `tests/fixtures/`
- Test: `tests/test_sources.py`

**Interfaces:**
- Produces: `Source.fetch() -> list[Observation]` for KNBS, CBK and World Bank.

- [ ] Write parser tests using official-page response fixtures for KNBS CPI, CBK daily USD/KES, and World Bank indicator JSON.
- [ ] Verify tests fail.
- [ ] Implement minimal parsers and HTTP fetchers with timeout/retry behavior.
- [ ] Verify tests pass.

### Task 3: BigQuery append-only warehouse adapter
**Files:**
- Create: `pipeline/warehouse.py`
- Create: `infra/bigquery/bootstrap.sql`
- Test: `tests/test_warehouse.py`

**Interfaces:**
- Consumes: enriched `Observation` objects.
- Produces: `BigQueryWarehouse.append_new(observations) -> int` and metadata run-record methods.

- [ ] Write tests proving existing hashes are excluded and only unseen revisions are loaded.
- [ ] Verify tests fail.
- [ ] Implement BigQuery query/load adapter with dependency-injected client for tests.
- [ ] Verify tests pass.

### Task 4: Refresh orchestration
**Files:**
- Create: `pipeline/refresh.py`
- Create: `pipeline/__main__.py`
- Test: `tests/test_refresh.py`

**Interfaces:**
- Produces: `run_refresh(sources, warehouse, dbt_runner, now) -> RunResult`.

- [ ] Write tests for success, one-source-degraded, and dbt-failure outcomes.
- [ ] Verify tests fail.
- [ ] Implement source-isolated execution, row counts and status calculation.
- [ ] Verify tests pass.

### Task 5: dbt BigQuery models and contracts
**Files:**
- Replace: `kenya_econ_dbt/profiles.yml`
- Replace/update: `kenya_econ_dbt/dbt_project.yml`
- Create/replace staging and mart models plus schema tests.

**Interfaces:**
- Produces: `staging.stg_economic_observations`, `marts.indicator_history`, `marts.latest_indicators`, `marts.economic_snapshot`, `marts.source_health`.

- [ ] Add dbt source/model schema tests first and verify `dbt parse`/test compilation identifies missing models.
- [ ] Implement models using BigQuery SQL and `row_number()` revision resolution.
- [ ] Verify `dbt parse` succeeds locally without warehouse execution.

### Task 6: Cloud Run container and runtime configuration
**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`
- Replace: `requirements.txt`

**Interfaces:**
- Container command executes `python -m pipeline`.

- [ ] Add a container smoke-test command to CI expectations.
- [ ] Build image locally.
- [ ] Run Python unit tests in the built environment.

### Task 7: GitHub Actions CI/CD and GCP bootstrap commands
**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `.github/workflows/deploy.yml`
- Create: `infra/gcp/bootstrap.sh`

**Interfaces:**
- CI validates Python + dbt parse.
- Deploy authenticates with `google-github-actions/auth@v3`, builds/pushes image, deploys a Cloud Run Job with `google-github-actions/deploy-cloudrun@v3`.

- [ ] Add workflow syntax/config validation.
- [ ] Implement WIF-based deployment with repository variables for project/region/provider/service account.
- [ ] Add bootstrap commands for APIs, Artifact Registry, service accounts, WIF, Scheduler and IAM.

### Task 8: Streamlit dashboard migration
**Files:**
- Replace: `dashboard/app.py`
- Replace: `dashboard/requirements.txt`

**Interfaces:**
- Reads BigQuery marts only.

- [ ] Extract dashboard query functions and write tests for empty/stale/current data states where practical.
- [ ] Replace Postgres connection code with BigQuery client.
- [ ] Add data-health section and remove live/Kafka claims.
- [ ] Run dashboard import/smoke test.

### Task 9: Remove legacy runtime and rewrite operational documentation
**Files:**
- Delete: `dags/`, `streaming/`, `docker-compose.yml`, `airflow.cfg`, `airflow-webserver.pid`, Postgres-only runtime files.
- Replace: `README.md`
- Update: `.gitignore`, `Makefile` if retained.

- [ ] Verify no production imports/references to Airflow, Kafka, ZooKeeper, OpenExchangeRates or psycopg remain.
- [ ] Document architecture decision history, local test commands, GCP setup, deployment, freshness semantics and cost assumptions.
- [ ] Run full verification suite.
