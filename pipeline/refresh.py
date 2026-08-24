from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable


@dataclass(frozen=True)
class RunResult:
    run_id: str
    status: str
    sources_succeeded: int
    sources_failed: int
    rows_inserted: int
    dbt_status: str
    error_message: str | None


def run_dbt() -> None:
    subprocess.run(
        ["dbt", "build", "--project-dir", "kenya_econ_dbt", "--profiles-dir", "kenya_econ_dbt", "--target", "prod"],
        check=True,
    )


def run_refresh(*, sources: Iterable, warehouse, dbt_runner: Callable[[], None] = run_dbt,
                now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
                run_id_factory: Callable[[], str] = lambda: str(uuid.uuid4()),
                git_sha: str | None = None) -> RunResult:
    run_id = run_id_factory()
    started_at = now()
    warehouse.start_run(run_id, started_at, git_sha)
    source_success = 0
    source_fail = 0
    rows_inserted = 0
    errors: list[str] = []

    for source in sources:
        checked_at = now()
        try:
            observations = source.fetch()
            enriched = [item.with_ingestion(run_id, checked_at) for item in observations]
            inserted = warehouse.append_new(enriched)
            rows_inserted += inserted
            source_success += 1
            latest_period = max((item.period_end for item in observations), default=None)
            warehouse.record_source_run(
                run_id=run_id,
                source=source.name,
                status="success",
                checked_at=checked_at,
                rows_fetched=len(observations),
                rows_inserted=inserted,
                latest_period=latest_period.isoformat() if latest_period else None,
                error_message=None,
            )
        except Exception as exc:  # Source isolation is intentional.
            source_fail += 1
            message = f"{source.name}: {exc}"
            errors.append(message)
            warehouse.record_source_run(
                run_id=run_id,
                source=source.name,
                status="failed",
                checked_at=checked_at,
                rows_fetched=0,
                rows_inserted=0,
                latest_period=None,
                error_message=str(exc),
            )

    dbt_status = "success"
    try:
        dbt_runner()
    except Exception as exc:
        dbt_status = "failed"
        errors.append(f"dbt: {exc}")

    if dbt_status == "failed" or source_success == 0:
        status = "failed"
    elif source_fail:
        status = "degraded"
    else:
        status = "success"

    completed_at = now()
    error_message = " | ".join(errors) if errors else None
    warehouse.finish_run(
        run_id=run_id,
        completed_at=completed_at,
        status=status,
        sources_succeeded=source_success,
        sources_failed=source_fail,
        rows_inserted=rows_inserted,
        dbt_status=dbt_status,
        error_message=error_message,
    )
    return RunResult(
        run_id=run_id,
        status=status,
        sources_succeeded=source_success,
        sources_failed=source_fail,
        rows_inserted=rows_inserted,
        dbt_status=dbt_status,
        error_message=error_message,
    )
