from __future__ import annotations

import calendar
import re
from datetime import date
from decimal import Decimal
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from pipeline.models import Observation
from pipeline.sources.base import HttpSource

CATEGORY_URL = "https://www.knbs.or.ke/reports_category/cpi-and-inflation-rates/"


def find_cpi_reports(category_html: str, limit: int = 12) -> list[str]:
    soup = BeautifulSoup(category_html, "html.parser")
    candidates: dict[str, date] = {}
    for anchor in soup.find_all("a", href=True):
        href = str(anchor["href"])
        text = anchor.get_text(" ", strip=True)
        combined = f"{text} {href}".lower()
        if "consumer-price-indices-and-inflation-rates" not in combined:
            continue
        match = re.search(
            r"(january|february|march|april|may|june|july|august|september|october|november|december)[-\s](20\d{2})",
            combined,
        )
        if not match:
            continue
        month = _month_number(match.group(1))
        candidates[href] = date(int(match.group(2)), month, 1)
    ordered = sorted(candidates.items(), key=lambda item: item[1], reverse=True)
    return [href for href, _ in ordered[:limit]]


def find_latest_cpi_report(category_html: str) -> str:
    reports = find_cpi_reports(category_html, limit=1)
    if not reports:
        raise ValueError("No KNBS CPI report link found")
    return reports[0]


def parse_cpi_report(report_html: str, source_url: str) -> Observation:
    text = BeautifulSoup(report_html, "html.parser").get_text(" ", strip=True)
    match = re.search(
        r"(?:Annual\s+consumer\s+price\s+inflation(?:\s+as\s+measured\s+by\s+the\s+Consumer\s+Price\s+Index\s*\(CPI\))?\s+was|Annual\s+consumer\s+price\s+inflation\s+was)\s+([0-9]+(?:\.[0-9]+)?)\s+per\s+cent\s+in\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError("Could not parse headline inflation from KNBS CPI report")
    value = Decimal(match.group(1))
    month = _month_number(match.group(2))
    year = int(match.group(3))
    last_day = calendar.monthrange(year, month)[1]
    return Observation(
        source="KNBS",
        indicator_code="CPI_INFLATION_YOY",
        indicator_name="Headline consumer price inflation",
        geography="Kenya",
        period_start=date(year, month, 1),
        period_end=date(year, month, last_day),
        frequency="monthly",
        value=value,
        unit="percent",
        currency=None,
        source_published_at=None,
        source_url=source_url,
        raw_payload={"report_text": text},
    )


def _month_number(name: str) -> int:
    months = {month.lower(): index for index, month in enumerate(calendar.month_name) if month}
    return months[name.lower()]


class KNBSSource(HttpSource):
    name = "KNBS"

    def fetch(self) -> list[Observation]:
        category_html = self.get_text(CATEGORY_URL)
        observations: list[Observation] = []
        for href in find_cpi_reports(category_html, limit=12):
            report_url = urljoin(CATEGORY_URL, href)
            report_html = self.get_text(report_url)
            observations.append(parse_cpi_report(report_html, report_url))
        if not observations:
            raise ValueError("No KNBS CPI observations discovered")
        return observations
