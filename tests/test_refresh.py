from datetime import date, datetime, timezone
from decimal import Decimal

from pipeline.models import Observation
from pipeline.refresh import run_refresh


class StubSource:
    def __init__(self, name, observations=None, error=None):
        self.name = name
        self._observations = observations or []
        self._error = error

    def fetch(self):
        if self._error:
            raise self._error
        return self._observations


class StubWarehouse:
    def __init__(self):
        self.started = []
        self.sources = []
        self.finished = []

    def start_run(self, run_id, started_at, git_sha):
        self.started.append(run_id)

    def append_new(self, observations):
        return len(observations)

    def record_source_run(self, **kwargs):
        self.sources.append(kwargs)

    def finish_run(self, **kwargs):
        self.finished.append(kwargs)


def obs():
    return Observation(
        source="CBK", indicator_code="USD_KES", indicator_name="FX", geography="Kenya",
        period_start=date(2026, 8, 21), period_end=date(2026, 8, 21), frequency="daily",
        value=Decimal("129.47"), unit="KES per USD", currency="KES",
        source_published_at=None, source_url="https://www.centralbank.go.ke/", raw_payload={"rate": 129.47},
    )


def test_refresh_is_success_when_all_sources_and_dbt_succeed():
    warehouse = StubWarehouse()
    result = run_refresh(
        sources=[StubSource("CBK", [obs()])],
        warehouse=warehouse,
        dbt_runner=lambda: None,
        now=lambda: datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc),
        run_id_factory=lambda: "run-ok",
        git_sha="abc",
    )
    assert result.status == "success"
    assert result.rows_inserted == 1


def test_refresh_is_degraded_when_one_source_fails_but_dbt_succeeds():
    warehouse = StubWarehouse()
    result = run_refresh(
        sources=[StubSource("CBK", [obs()]), StubSource("KNBS", error=RuntimeError("source down"))],
        warehouse=warehouse,
        dbt_runner=lambda: None,
        now=lambda: datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc),
        run_id_factory=lambda: "run-degraded",
        git_sha="abc",
    )
    assert result.status == "degraded"
    assert result.sources_failed == 1
    assert len(warehouse.sources) == 2


def test_refresh_is_failed_when_dbt_fails():
    warehouse = StubWarehouse()

    def fail_dbt():
        raise RuntimeError("dbt failed")

    result = run_refresh(
        sources=[StubSource("CBK", [obs()])],
        warehouse=warehouse,
        dbt_runner=fail_dbt,
        now=lambda: datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc),
        run_id_factory=lambda: "run-failed",
        git_sha="abc",
    )
    assert result.status == "failed"
    assert result.dbt_status == "failed"
