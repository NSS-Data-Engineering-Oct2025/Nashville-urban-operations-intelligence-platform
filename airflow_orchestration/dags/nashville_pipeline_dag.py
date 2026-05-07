from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_arguments = {
    "owner": "nashville_data_engineering_project",
    "start_date": datetime(2026, 5, 1),
    "retries": 1,
}


with DAG(
    dag_id="nashville_urban_operations_pipeline",
    default_args=default_arguments,
    schedule_interval="@daily",
    catchup=False,
    description="End-to-end Nashville urban operations pipeline: API to S3 to Snowflake to dbt",
) as dag:

    run_api_data_ingestion = BashOperator(
        task_id="run_api_data_ingestion",
        bash_command="cd /opt/airflow/project && python api_data_ingestion/nashville_api_data_ingestion.py",
    )

    load_data_into_snowflake = BashOperator(
        task_id="load_data_into_snowflake",
        bash_command="cd /opt/airflow/project && python snowflake_data_warehouse/load_data_into_snowflake.py",
    )

    transform_raw_to_bronze = BashOperator(
        task_id="transform_raw_to_bronze",
        bash_command="cd /opt/airflow/project && python snowflake_data_warehouse/transform_raw_to_bronze.py",
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/project/nashville_data_transformation && "
            "dbt run --profiles-dir /opt/airflow/project/nashville_data_transformation"
        ),
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            "cd /opt/airflow/project/nashville_data_transformation && "
            "dbt test --profiles-dir /opt/airflow/project/nashville_data_transformation"
        ),
    )

    (
        run_api_data_ingestion
        >> load_data_into_snowflake
        >> transform_raw_to_bronze
        >> run_dbt_models
        >> run_dbt_tests
    )