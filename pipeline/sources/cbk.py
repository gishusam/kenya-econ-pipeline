from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal

from bs4 import BeautifulSoup

from pipeline.models import Observation
from pipeline.sources.base import HttpSource

CBK_URL = "https://www.centralbank.go.ke/"


def parse_daily_usd_kes(html: str, source_url: str) -> Observation:
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    rate_match = re.search(r"US\s+DOLLAR\s*\|?\s*([0-9]+(?:\.[0-9]+)?)", text, flags=re.IGNORECASE)
    date_match = re.search(r"Posted\s+On:\s*(\d{2}-\d{2}-\d{4})", text, flags=re.IGNORECASE)
    if not rate_match or not date_match:
        raise ValueError("Could not parse CBK daily USD/KES rate")
    observed_date = datetime.strptime(date_match.group(1), "%d-%m-%Y").date()
    rate = Decimal(rate_match.group(1))
    return Observation(
        source="CBK",
        indicator_code="USD_KES",
        indicator_name="Kenya shilling per US dollar",
        geography="Kenya",
        period_start=observed_date,
        period_end=observed_date,
        frequency="daily",
        value=rate,
        unit="KES per USD",
        currency="KES",
        source_published_at=None,
        source_url=source_url,
        raw_payload={"page_text": text},
    )


class CBKSource(HttpSource):
    name = "CBK"

    def fetch(self) -> list[Observation]:
        return [parse_daily_usd_kes(self.get_text(CBK_URL), CBK_URL)]
