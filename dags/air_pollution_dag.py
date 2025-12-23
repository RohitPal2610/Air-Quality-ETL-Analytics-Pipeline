from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="air_pollution_pipeline",
    default_args=default_args,
    description="Air Pollution ETL Pipeline using PySpark",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    ingest = BashOperator(
        task_id="merge_raw_data",
        bash_command="python /opt/airflow/spark/merge.py"
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/spark/transform.py"
    )

    analyze = BashOperator(
        task_id="analyze_and_visualize",
        bash_command="python /opt/airflow/spark/analysis_viz.py"
    )

    ingest >> transform >> analyze
