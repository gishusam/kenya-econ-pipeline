from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Observation:
    source: str
    indicator_code: str
    indicator_name: str
    geography: str
    period_start: date
    period_end: date
    frequency: str
    value: Decimal
    unit: str
    currency: str | None
    source_published_at: datetime | None
    source_url: str
    raw_payload: dict[str, Any]

    def with_ingestion(self, run_id: str, ingested_at: datetime) -> "IngestedObservation":
        from pipeline.hashing import record_hash

        return IngestedObservation(
            observation=self,
            source_record_hash=record_hash(self),
            run_id=run_id,
            ingested_at=ingested_at,
        )


@dataclass(frozen=True)
class IngestedObservation:
    observation: Observation
    source_record_hash: str
    run_id: str
    ingested_at: datetime

    def to_bigquery_row(self) -> dict[str, Any]:
        o = self.observation
        return {
            "source": o.source,
            "indicator_code": o.indicator_code,
            "indicator_name": o.indicator_name,
            "geography": o.geography,
            "period_start": o.period_start.isoformat(),
            "period_end": o.period_end.isoformat(),
            "frequency": o.frequency,
            "value": _normalize_decimal(o.value),
            "unit": o.unit,
            "currency": o.currency,
            "source_published_at": o.source_published_at.isoformat() if o.source_published_at else None,
            "source_url": o.source_url,
            "source_record_hash": self.source_record_hash,
            "raw_payload": o.raw_payload,
            "ingested_at": self.ingested_at.isoformat(),
            "run_id": self.run_id,
        }


def _normalize_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"
