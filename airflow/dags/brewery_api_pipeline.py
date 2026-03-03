from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from brewery_pipeline.ingest import run

with DAG(
    dag_id="brewery_api_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["api", "postgres"],
) as dag:

    ingest_task = PythonOperator(
        task_id="run_ingestion",
        python_callable=run,
        retries=3,
    )