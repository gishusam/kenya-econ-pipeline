# Autonomous Kenya Economic Intelligence — Design

## Goal
Rebuild the shelved Kenya economic pipeline as an autonomous, low-maintenance data product that remains useful without a developer laptop or always-on self-hosted infrastructure.

## Architecture
- Warehouse: Google BigQuery.
- Transformations and data contracts: dbt-bigquery.
- Runtime: one Cloud Run Job named `kenya-econ-refresh`.
- Scheduling: Google Cloud Scheduler triggers the Cloud Run Job daily.
- CI/CD: GitHub Actions validates pull requests and deploys the Cloud Run Job on `main`.
- Dashboard: Streamlit Community Cloud querying BigQuery marts only.
- No Airflow and no Kafka in the production path.

## Sources
- KNBS: official CPI release pages, initially annual headline inflation observations.
- CBK: official website daily KES/USD exchange rate.
- World Bank: official JSON API for historical/annual macro indicators, initially real GDP growth and GDP in current local currency.

Each source is low-volume and batch-oriented. A daily job checks for new or revised observations; unchanged records are not re-appended.

## Warehouse model
BigQuery datasets:
- `raw`: append-only economic observations with source payload, source URL, deterministic record hash, ingestion timestamp and run ID.
- `staging`: dbt-standardized observations retaining revisions and marking the latest revision.
- `marts`: dashboard-facing latest observations and economic history using the latest revision per natural key.
- `metadata`: pipeline and source execution state.

Raw rows are never updated or deleted by the application. A revised source value produces a new hash and therefore a new raw row.

## Canonical observation
Every extractor emits:
- source
- indicator_code
- indicator_name
- geography
- period_start
- period_end
- frequency
- value
- unit
- currency (nullable)
- source_published_at (nullable)
- source_url
- raw_payload

The pipeline adds:
- source_record_hash
- ingested_at
- run_id

The natural revision key is `(source, indicator_code, geography, period_start, period_end)`.

## Execution flow
1. Create `run_id` and record pipeline start.
2. Check KNBS, CBK and World Bank independently.
3. Parse and validate each source into canonical observations.
4. Compute deterministic hashes and append only unseen hashes to BigQuery raw.
5. Record per-source status and row counts.
6. Run `dbt build --target prod`.
7. Record final run status: `success`, `degraded`, or `failed`.

A source failure does not erase successful work from other sources. dbt failure makes the overall run failed. Source failures make the run degraded if dbt succeeds with previously valid data.

## Failure and freshness rules
- Never silently represent stale data as fresh.
- A failed source check records failure metadata and leaves existing warehouse data intact.
- Dashboard marts expose source observation date and ingestion/run freshness.
- Streamlit displays source health and last successful pipeline run.

## Security
- Cloud Run uses a dedicated least-privilege service account.
- GitHub Actions authenticates to Google Cloud through Workload Identity Federation; no service-account JSON key is stored in GitHub.
- Streamlit gets a separate read-only BigQuery identity/credential.

## Deployment
A single container image contains the Python pipeline and dbt project. GitHub Actions builds/pushes the image and deploys a new Cloud Run Job revision. Cloud Scheduler invokes the deployed job; GitHub cron is not used for production scheduling.

## Explicit removals from the original architecture
- Remove Airflow DAG/config/PID and Airflow dependencies.
- Remove Kafka/ZooKeeper/producer/consumer/topic setup and Kafka dependency.
- Remove Postgres-specific migrations/loaders/runtime configuration from the production path.
- Remove OpenExchangeRates from the critical path; CBK becomes authoritative for KES FX.

## Non-goals for this rebuild
- Forecasting or ML.
- Real-time streaming.
- Kubernetes.
- Managed Airflow.
- A generic multi-country economic platform.
- Complex IaC before the first autonomous deployment works.
