from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "samwel",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

def run_migrations():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from db.migrate import run_migrations as migrate
    migrate()

def run_worldbank_ingestion():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.worldbank import WorldBankIngester
    WorldBankIngester().run()

def run_fx_ingestion():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.fx_rates import FXRatesIngester
    FXRatesIngester().run()

def run_loaders():
    import sys
    sys.path.insert(0, "/opt/airflow")
    from db.loaders.worldbank_loader import run as run_worldbank
    from db.loaders.fx_loader import run as run_fx
    run_worldbank()
    run_fx()

with DAG(
    dag_id="kenya_econ_pipeline",
    description="Daily Kenya economic data pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kenya", "economics", "pipeline"],
) as dag:

    migrate_db = PythonOperator(
        task_id="migrate_db",
        python_callable=run_migrations,
    )

    worldbank_ingest = PythonOperator(
        task_id="worldbank_ingest",
        python_callable=run_worldbank_ingestion,
    )

    fx_ingest = PythonOperator(
        task_id="fx_ingest",
        python_callable=run_fx_ingestion,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=run_loaders,
        sla=timedelta(minutes=60),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/kenya_econ_dbt && dbt run --profiles-dir /opt/airflow/kenya_econ_dbt",
        sla=timedelta(minutes=60),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/kenya_econ_dbt && dbt test --profiles-dir /opt/airflow/kenya_econ_dbt",
    )

    migrate_db >> [worldbank_ingest, fx_ingest] >> load_to_postgres >> dbt_run >> dbt_test # type: ignore