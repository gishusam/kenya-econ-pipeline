CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.raw` OPTIONS(location="__BQ_LOCATION__");
CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.staging` OPTIONS(location="__BQ_LOCATION__");
CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.marts` OPTIONS(location="__BQ_LOCATION__");
CREATE SCHEMA IF NOT EXISTS `__PROJECT_ID__.metadata` OPTIONS(location="__BQ_LOCATION__");

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.raw.economic_observations` (
  source STRING NOT NULL,
  indicator_code STRING NOT NULL,
  indicator_name STRING NOT NULL,
  geography STRING NOT NULL,
  period_start DATE NOT NULL,
  period_end DATE NOT NULL,
  frequency STRING NOT NULL,
  value NUMERIC NOT NULL,
  unit STRING NOT NULL,
  currency STRING,
  source_published_at TIMESTAMP,
  source_url STRING NOT NULL,
  source_record_hash STRING NOT NULL,
  raw_payload JSON,
  ingested_at TIMESTAMP NOT NULL,
  run_id STRING NOT NULL
)
PARTITION BY DATE(ingested_at)
CLUSTER BY source, indicator_code, geography;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.metadata.pipeline_runs` (
  run_id STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  completed_at TIMESTAMP,
  status STRING NOT NULL,
  sources_succeeded INT64 NOT NULL,
  sources_failed INT64 NOT NULL,
  rows_inserted INT64 NOT NULL,
  dbt_status STRING NOT NULL,
  git_sha STRING,
  error_message STRING
)
PARTITION BY DATE(started_at)
CLUSTER BY status;

CREATE TABLE IF NOT EXISTS `__PROJECT_ID__.metadata.source_runs` (
  run_id STRING NOT NULL,
  source STRING NOT NULL,
  status STRING NOT NULL,
  checked_at TIMESTAMP NOT NULL,
  rows_fetched INT64 NOT NULL,
  rows_inserted INT64 NOT NULL,
  latest_period DATE,
  error_message STRING
)
PARTITION BY DATE(checked_at)
CLUSTER BY source, status;
