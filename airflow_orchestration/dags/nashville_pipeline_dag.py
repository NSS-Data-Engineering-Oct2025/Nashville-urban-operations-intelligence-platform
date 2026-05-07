from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "zia",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="nashville_urban_operations_pipeline",
    default_args=default_args,
    description="Nashville Urban Operations Intelligence Platform Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=[
        "nashville",
        "snowflake",
        "dbt",
        "streamlit",
        "data-engineering"
    ]
) as dag:


    ingest_api_data = BashOperator(
        task_id="ingest_api_data",
        bash_command="""
        cd /opt/airflow/project &&
        python api_data_ingestion/nashville_api_data_ingestion.py
        """
    )


    load_data_into_snowflake = BashOperator(
        task_id="load_data_into_snowflake",
        bash_command="""
        cd /opt/airflow/project &&
        python snowflake_data_warehouse/load_data_into_snowflake.py
        """
    )


    transform_bronze_to_silver = BashOperator(
        task_id="transform_bronze_to_silver",
        bash_command="""
        cd /opt/airflow/project &&
        python snowflake_data_warehouse/transform_bronze_to_silver.py
        """
    )


    transform_silver_to_gold = BashOperator(
        task_id="transform_silver_to_gold",
        bash_command="""
        cd /opt/airflow/project &&
        python snowflake_data_warehouse/transform_silver_to_gold.py
        """
    )


    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
        cd /opt/airflow/project/nashville_data_transformation &&
        dbt run --profiles-dir .
        """
    )


    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command="""
        cd /opt/airflow/project/nashville_data_transformation &&
        dbt test --profiles-dir .
        """
    )


    ingest_api_data >> load_data_into_snowflake

    load_data_into_snowflake >> transform_bronze_to_silver

    transform_bronze_to_silver >> transform_silver_to_gold

    transform_silver_to_gold >> run_dbt_models

    run_dbt_models >> run_dbt_tests