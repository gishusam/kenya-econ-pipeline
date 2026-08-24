import json
from datetime import date
from decimal import Decimal
from pathlib import Path

from pipeline.sources.cbk import parse_daily_usd_kes
from pipeline.sources.knbs import find_cpi_reports, find_latest_cpi_report, parse_cpi_report
from pipeline.sources.world_bank import parse_indicator_response

FIXTURES = Path(__file__).parent / "fixtures"


def test_knbs_finds_latest_cpi_report_and_parses_headline_inflation():
    category = (FIXTURES / "knbs_cpi_category.html").read_text()
    report = (FIXTURES / "knbs_cpi_report.html").read_text()

    report_url = find_latest_cpi_report(category)
    observation = parse_cpi_report(report, report_url)

    assert report_url.endswith("july-2026/")
    assert observation.source == "KNBS"
    assert observation.indicator_code == "CPI_INFLATION_YOY"
    assert observation.value == Decimal("6.5")
    assert observation.period_start == date(2026, 7, 1)
    assert observation.period_end == date(2026, 7, 31)


def test_cbk_parses_official_daily_usd_kes_rate_and_posted_date():
    html = (FIXTURES / "cbk_home.html").read_text()

    observation = parse_daily_usd_kes(html, "https://www.centralbank.go.ke/")

    assert observation.source == "CBK"
    assert observation.indicator_code == "USD_KES"
    assert observation.value == Decimal("129.47")
    assert observation.period_start == date(2026, 8, 21)
    assert observation.period_end == date(2026, 8, 21)


def test_world_bank_maps_official_indicator_to_canonical_observations():
    payload = json.loads((FIXTURES / "world_bank.json").read_text())

    observations = parse_indicator_response(payload, "NY.GDP.MKTP.KD.ZG")

    assert len(observations) == 2
    assert observations[0].indicator_code == "REAL_GDP_GROWTH"
    assert observations[0].value == Decimal("4.7")
    assert observations[0].period_start == date(2025, 1, 1)
    assert observations[0].period_end == date(2025, 12, 31)


def test_knbs_discovers_recent_report_links_in_newest_first_order():
    category = (FIXTURES / "knbs_cpi_category.html").read_text()
    reports = find_cpi_reports(category, limit=12)
    assert len(reports) == 2
    assert reports[0].endswith("july-2026/")
    assert reports[1].endswith("june-2026/")


def test_world_bank_fetches_full_available_history_not_only_recent_values():
    from pipeline.sources.world_bank import WorldBankSource

    class StubWorldBankSource(WorldBankSource):
        def __init__(self):
            self.calls = []

        def get_json(self, url, params=None):
            self.calls.append((url, params))
            return [{"page": 1}, []]

    source = StubWorldBankSource()
    source.fetch()

    assert len(source.calls) == 2
    for _, params in source.calls:
        assert params["per_page"] >= 100
        assert "mrv" not in params
