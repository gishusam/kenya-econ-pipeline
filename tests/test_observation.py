from datetime import date, datetime, timezone
from decimal import Decimal

from pipeline.hashing import record_hash
from pipeline.models import Observation


def make_observation(value: str = "6.5") -> Observation:
    return Observation(
        source="KNBS",
        indicator_code="CPI_INFLATION_YOY",
        indicator_name="Headline consumer price inflation",
        geography="Kenya",
        period_start=date(2026, 7, 1),
        period_end=date(2026, 7, 31),
        frequency="monthly",
        value=Decimal(value),
        unit="percent",
        currency=None,
        source_published_at=None,
        source_url="https://www.knbs.or.ke/reports/cpi-july-2026/",
        raw_payload={"headline": "July 2026"},
    )


def test_record_hash_is_stable_for_equivalent_observations():
    first = make_observation("6.50")
    second = make_observation("6.500")

    assert record_hash(first) == record_hash(second)


def test_record_hash_changes_when_observed_value_changes():
    assert record_hash(make_observation("6.5")) != record_hash(make_observation("6.6"))


def test_with_ingestion_adds_operational_metadata_without_mutating_observation():
    observed = make_observation()
    ingested_at = datetime(2026, 8, 24, 14, 0, tzinfo=timezone.utc)

    enriched = observed.with_ingestion("run-123", ingested_at)

    assert enriched.run_id == "run-123"
    assert enriched.ingested_at == ingested_at
    assert enriched.source_record_hash == record_hash(observed)
    assert observed.value == Decimal("6.5")
