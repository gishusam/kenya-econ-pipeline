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


def test_knbs_builds_verified_ca_bundle_with_missing_intermediates(tmp_path):
    from pipeline.sources.knbs import build_knbs_ca_bundle

    base = tmp_path / "base.pem"
    extra = tmp_path / "extra.pem"
    output = tmp_path / "combined.pem"

    base.write_text("BASE-CA\n")
    extra.write_text("KNBS-EXTRA-CA\n")

    result = build_knbs_ca_bundle(
        base_bundle=base,
        extra_bundle=extra,
        output_bundle=output,
    )

    assert result == output
    assert output.read_text() == "BASE-CA\nKNBS-EXTRA-CA\n"


def test_knbs_session_uses_combined_verified_ca_bundle(tmp_path):
    from pipeline.sources.knbs import build_knbs_session

    base = tmp_path / "base.pem"
    extra = tmp_path / "extra.pem"
    output = tmp_path / "combined.pem"

    base.write_text("BASE-CA\n")
    extra.write_text("KNBS-EXTRA-CA\n")

    session = build_knbs_session(
        base_bundle=base,
        extra_bundle=extra,
        output_bundle=output,
    )

    assert session.verify == str(output)
    assert output.read_text() == "BASE-CA\nKNBS-EXTRA-CA\n"


def test_knbs_source_uses_verified_custom_ca_session_by_default():
    from pipeline.sources.knbs import KNBSSource

    source = KNBSSource()

    assert source.session.verify is not True
    assert str(source.session.verify).endswith(
        "kenya-econ-knbs-ca-bundle.pem"
    )


def test_knbs_combines_current_reports_with_cpi_archive():
    from pipeline.sources.knbs import (
        ALL_REPORTS_URL,
        CATEGORY_URL,
        discover_cpi_reports,
    )

    current_html = """
    <a href="https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-july-2026/">
        Consumer Price Indices and Inflation Rates – July 2026
    </a>
    """

    archive_html = """
    <a href="https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-june-2026/">
        Consumer Price Indices and Inflation Rates – June 2026
    </a>
    <a href="https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-may-2026/">
        Consumer Price Indices and Inflation Rates – May 2026
    </a>
    """

    pages = {
        ALL_REPORTS_URL: current_html,
        CATEGORY_URL: archive_html,
    }

    reports = discover_cpi_reports(
        get_text=lambda url: pages[url],
        limit=12,
    )

    assert reports == [
        "https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-july-2026/",
        "https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-june-2026/",
        "https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-may-2026/",
    ]


def test_knbs_report_discovery_ignores_pdf_attachments():
    from pipeline.sources.knbs import find_cpi_reports

    html = """
    <a href="https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-july-2026/">
        Consumer Price Indices and Inflation Rates – July 2026
    </a>

    <a href="https://www.knbs.or.ke/wp-content/uploads/2026/07/Kenya-Consumer-Price-Indices-and-Inflation-Rates-July-2026.pdf">
        Consumer Price Indices and Inflation Rates – July 2026 PDF
    </a>
    """

    reports = find_cpi_reports(html, limit=12)

    assert reports == [
        "https://www.knbs.or.ke/reports/consumer-price-indices-and-inflation-rates-july-2026/"
    ]
