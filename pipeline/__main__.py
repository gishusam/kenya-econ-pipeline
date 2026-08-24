from __future__ import annotations

import os
import sys

from pipeline.refresh import run_refresh
from pipeline.sources import CBKSource, KNBSSource, WorldBankSource
from pipeline.warehouse import BigQueryWarehouse


def exit_code_for_status(status: str) -> int:
    return {"success": 0, "degraded": 2, "failed": 1}.get(status, 1)


def main() -> int:
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise RuntimeError("GCP_PROJECT_ID must be set")

    from google.cloud import bigquery

    client = bigquery.Client(project=project_id, location=os.getenv("BQ_LOCATION", "africa-south1"))
    warehouse = BigQueryWarehouse(client=client, project_id=project_id)
    result = run_refresh(
        sources=[KNBSSource(), CBKSource(), WorldBankSource()],
        warehouse=warehouse,
        git_sha=os.getenv("GIT_SHA"),
    )
    print(
        f"run_id={result.run_id} status={result.status} "
        f"sources_ok={result.sources_succeeded} sources_failed={result.sources_failed} "
        f"rows_inserted={result.rows_inserted} dbt={result.dbt_status}"
    )
    return exit_code_for_status(result.status)


if __name__ == "__main__":
    sys.exit(main())
