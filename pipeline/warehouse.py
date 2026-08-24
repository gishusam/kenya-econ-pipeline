from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

from pipeline.models import IngestedObservation

try:
    from google.cloud import bigquery as _bigquery
except ImportError:  # Allows pure unit tests without GCP SDK installed.
    _bigquery = None


@dataclass
class _ArrayParameter:
    values: list[str]


@dataclass
class _QueryConfig:
    query_parameters: list[_ArrayParameter]


class BigQueryWarehouse:
    def __init__(self, client, project_id: str, raw_dataset: str = "raw", metadata_dataset: str = "metadata"):
        self.client = client
        self.project_id = project_id
        self.raw_table = f"{project_id}.{raw_dataset}.economic_observations"
        self.pipeline_runs_table = f"{project_id}.{metadata_dataset}.pipeline_runs"
        self.source_runs_table = f"{project_id}.{metadata_dataset}.source_runs"

    def append_new(self, observations: Iterable[IngestedObservation]) -> int:
        observations = list(observations)
        if not observations:
            return 0
        hashes = [item.source_record_hash for item in observations]
        existing = self._existing_hashes(hashes)
        rows = [item.to_bigquery_row() for item in observations if item.source_record_hash not in existing]
        if not rows:
            return 0
        job_config = _bigquery.LoadJobConfig(write_disposition="WRITE_APPEND") if _bigquery else None
        self.client.load_table_from_json(rows, self.raw_table, job_config=job_config).result()
        return len(rows)

    def _existing_hashes(self, hashes: list[str]) -> set[str]:
        if _bigquery:
            config = _bigquery.QueryJobConfig(
                query_parameters=[_bigquery.ArrayQueryParameter("hashes", "STRING", hashes)]
            )
        else:
            config = _QueryConfig(query_parameters=[_ArrayParameter(values=hashes)])
        query = f"SELECT source_record_hash FROM `{self.raw_table}` WHERE source_record_hash IN UNNEST(@hashes)"
        return {row.source_record_hash for row in self.client.query(query, job_config=config).result()}

    def start_run(self, run_id: str, started_at: datetime, git_sha: str | None) -> None:
        self._load_rows(self.pipeline_runs_table, [{
            "run_id": run_id,
            "started_at": started_at.isoformat(),
            "completed_at": None,
            "status": "running",
            "sources_succeeded": 0,
            "sources_failed": 0,
            "rows_inserted": 0,
            "dbt_status": "pending",
            "git_sha": git_sha,
            "error_message": None,
        }])

    def record_source_run(self, **row) -> None:
        serialized = {key: value.isoformat() if isinstance(value, datetime) else value for key, value in row.items()}
        self._load_rows(self.source_runs_table, [serialized])

    def finish_run(self, *, run_id: str, completed_at: datetime, status: str, sources_succeeded: int,
                   sources_failed: int, rows_inserted: int, dbt_status: str, error_message: str | None) -> None:
        if _bigquery:
            config = _bigquery.QueryJobConfig(query_parameters=[
                _bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
                _bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", completed_at),
                _bigquery.ScalarQueryParameter("status", "STRING", status),
                _bigquery.ScalarQueryParameter("sources_succeeded", "INT64", sources_succeeded),
                _bigquery.ScalarQueryParameter("sources_failed", "INT64", sources_failed),
                _bigquery.ScalarQueryParameter("rows_inserted", "INT64", rows_inserted),
                _bigquery.ScalarQueryParameter("dbt_status", "STRING", dbt_status),
                _bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            ])
            query = f"""
            UPDATE `{self.pipeline_runs_table}`
            SET completed_at=@completed_at, status=@status, sources_succeeded=@sources_succeeded,
                sources_failed=@sources_failed, rows_inserted=@rows_inserted,
                dbt_status=@dbt_status, error_message=@error_message
            WHERE run_id=@run_id
            """
            self.client.query(query, job_config=config).result()

    def _load_rows(self, table: str, rows: list[dict]) -> None:
        if not rows:
            return
        job_config = _bigquery.LoadJobConfig(write_disposition="WRITE_APPEND") if _bigquery else None
        self.client.load_table_from_json(rows, table, job_config=job_config).result()
