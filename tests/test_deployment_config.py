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
