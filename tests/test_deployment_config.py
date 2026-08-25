from pathlib import Path


def test_deploy_workflow_uses_dedicated_scheduler_region():
    text = Path('.github/workflows/deploy.yml').read_text()
    assert '${{ vars.GCP_SCHEDULER_REGION }}' in text
    assert '--location="${{ vars.GCP_SCHEDULER_REGION }}"' in text


def test_gcp_bootstrap_defaults_scheduler_to_supported_fallback_region():
    text = Path('infra/gcp/bootstrap.sh').read_text()
    assert 'SCHEDULER_REGION="${SCHEDULER_REGION:-europe-west1}"' in text
    assert 'GCP_SCHEDULER_REGION=${SCHEDULER_REGION}' in text


def test_source_health_keeps_failed_sources_with_no_observations():
    text = Path('kenya_econ_dbt/models/marts/source_health.sql').read_text().lower()
    assert 'full outer join' in text
    assert "when latest_observation_date is null then 'stale'" in text


def test_gitignore_blocks_local_streamlit_and_gcp_secret_files():
    text = Path('.gitignore').read_text()
    assert '.streamlit/secrets.toml' in text
    assert 'service-account*.json' in text


def test_makefile_passes_the_environment_names_dbt_profile_reads():
    text = Path('Makefile').read_text()
    assert 'GCP_PROJECT_ID=' in text
    assert 'BQ_LOCATION=' in text
    assert 'DBT_BIGQUERY_PROJECT=' not in text


def test_streamlit_requirements_include_bigquery_dataframe_types():
    text = Path('dashboard/requirements.txt').read_text()
    assert 'db-dtypes==1.7.1' in text


def test_raw_economic_value_supports_high_precision_source_values():
    text = Path('infra/bigquery/bootstrap.sql').read_text()
    assert 'value BIGNUMERIC NOT NULL' in text


def test_staging_revision_window_qualifies_source_column():
    text = Path(
        'kenya_econ_dbt/models/staging/stg_economic_observations.sql'
    ).read_text().lower()

    assert 'from source as src' in text
    normalized = ' '.join(text.split())
    assert 'partition by src.source' in normalized


def test_docker_image_includes_knbs_ca_chain():
    text = Path('Dockerfile').read_text()
    assert 'COPY certs ./certs' in text

def test_pipeline_status_is_materialized_as_view():
    sql = Path(
        "kenya_econ_dbt/models/marts/pipeline_status.sql"
    ).read_text()

    normalized = "".join(sql.split())

    assert 'config(materialized="view")' in normalized


def test_repository_has_root_streamlit_entrypoint():
    entrypoint = Path("streamlit_app.py")

    assert entrypoint.exists()

    content = entrypoint.read_text()

    assert '"dashboard"' in content
    assert '"app.py"' in content


def test_dashboard_uses_display_helpers():
    app = Path("dashboard/app.py").read_text()

    assert "prepare_snapshot_for_display" in app
    assert "prepare_health_for_display" in app
    assert "format_eat_timestamp" in app


def test_dashboard_uses_storytelling_layout():
    app = Path("dashboard/app.py").read_text()

    assert "build_economy_summary" in app
    assert "indicator_context" in app
    assert "recent_indicator_history" in app

    assert "Pipeline health" in app
    assert "Data freshness" in app
    assert "Kenya's economy at a glance" in app
    assert "What's changing now?" in app
    assert "Economic performance" in app
    assert "Sources & methodology" in app

    assert "st.sidebar" in app


def test_streamlit_entrypoint_executes_dashboard_on_every_rerun():
    entrypoint = Path("streamlit_app.py").read_text()

    # An import-only wrapper is unsafe for Streamlit reruns because
    # Python caches imported modules in sys.modules.
    assert "import dashboard.app" not in entrypoint
    assert "runpy.run_path" in entrypoint


def test_dashboard_uses_current_streamlit_width_api():
    app = Path("dashboard/app.py").read_text()

    assert "use_container_width" not in app
    assert 'width="stretch"' in app


def test_dashboard_uses_semantic_metric_changes():
    app = Path("dashboard/app.py").read_text()

    assert "metric_change_label" in app
    assert "delta=" not in app


def test_dashboard_uses_categorical_gdp_years():
    app = Path("dashboard/app.py").read_text()

    assert "annual_indicator_history" in app
    assert 'x="year"' in app


def test_dashboard_does_not_chart_sparse_fx_history():
    app = Path("dashboard/app.py").read_text()

    assert "has_enough_history" in app


def test_dashboard_finishing_touches_are_present():
    app = Path("dashboard/app.py").read_text()

    assert '[data-testid="stSidebar"] div[data-testid="stMetric"]' in app
    assert "history-building" in app
    assert 'display_text="Open ↗"' in app
    assert "previous_period_end" in app

    # Native Streamlit delta arrows are misleading for economic semantics.
    assert "delta=" not in app
