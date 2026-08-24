from __future__ import annotations

from datetime import date
from decimal import Decimal

from pipeline.models import Observation
from pipeline.sources.base import HttpSource

BASE_URL = "https://api.worldbank.org/v2/country/KE/indicator/{indicator}"
INDICATORS = {
    "NY.GDP.MKTP.KD.ZG": {
        "code": "REAL_GDP_GROWTH",
        "name": "Real GDP growth",
        "unit": "percent",
        "currency": None,
    },
    "NY.GDP.MKTP.CN": {
        "code": "GDP_CURRENT_LCU",
        "name": "GDP in current local currency",
        "unit": "LCU",
        "currency": "KES",
    },
}


def parse_indicator_response(payload, indicator_id: str) -> list[Observation]:
    if indicator_id not in INDICATORS:
        raise ValueError(f"Unsupported World Bank indicator: {indicator_id}")
    if not isinstance(payload, list) or len(payload) < 2 or not isinstance(payload[1], list):
        raise ValueError("Unexpected World Bank response shape")
    meta = INDICATORS[indicator_id]
    source_url = BASE_URL.format(indicator=indicator_id)
    observations: list[Observation] = []
    for record in payload[1]:
        if record.get("value") is None:
            continue
        year = int(record["date"])
        observations.append(
            Observation(
                source="WORLD_BANK",
                indicator_code=meta["code"],
                indicator_name=meta["name"],
                geography="Kenya",
                period_start=date(year, 1, 1),
                period_end=date(year, 12, 31),
                frequency="annual",
                value=Decimal(str(record["value"])),
                unit=meta["unit"],
                currency=meta["currency"],
                source_published_at=None,
                source_url=source_url,
                raw_payload=record,
            )
        )
    return observations


class WorldBankSource(HttpSource):
    name = "WORLD_BANK"

    def fetch(self) -> list[Observation]:
        observations: list[Observation] = []
        for indicator_id in INDICATORS:
            url = BASE_URL.format(indicator=indicator_id)
            payload = self.get_json(url, params={"format": "json", "per_page": 100})
            observations.extend(parse_indicator_response(payload, indicator_id))
        return observations
