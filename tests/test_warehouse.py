from datetime import date, datetime, timezone
from decimal import Decimal

from pipeline.models import Observation
from pipeline.warehouse import BigQueryWarehouse


class QueryJob:
    def __init__(self, hashes):
        self._hashes = hashes

    def result(self):
        return [type("Row", (), {"source_record_hash": value}) for value in self._hashes]


class LoadJob:
    def result(self):
        return None


class FakeClient:
    def __init__(self, existing_hashes=None):
        self.existing_hashes = set(existing_hashes or [])
        self.loaded_rows = []

    def query(self, query, job_config=None):
        requested = set(job_config.query_parameters[0].values)
        return QueryJob(requested & self.existing_hashes)

    def load_table_from_json(self, rows, table, job_config=None):
        self.loaded_rows.extend(rows)
        return LoadJob()


def observation(value: str):
    return Observation(
        source="CBK",
        indicator_code="USD_KES",
        indicator_name="Kenya shilling per US dollar",
        geography="Kenya",
        period_start=date(2026, 8, 21),
        period_end=date(2026, 8, 21),
        frequency="daily",
        value=Decimal(value),
        unit="KES per USD",
        currency="KES",
        source_published_at=None,
        source_url="https://www.centralbank.go.ke/",
        raw_payload={"rate": value},
    )


def test_append_new_skips_existing_hashes_and_loads_only_new_revision(monkeypatch):
    now = datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc)
    old = observation("129.47").with_ingestion("run-1", now)
    revised = observation("129.50").with_ingestion("run-2", now)
    client = FakeClient(existing_hashes={old.source_record_hash})
    warehouse = BigQueryWarehouse(client=client, project_id="demo")

    inserted = warehouse.append_new([old, revised])

    assert inserted == 1
    assert len(client.loaded_rows) == 1
    assert client.loaded_rows[0]["source_record_hash"] == revised.source_record_hash
    assert client.loaded_rows[0]["value"] == "129.5"
