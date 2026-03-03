from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from bronze import run as bronze_run
from silver import run as silver_run
from gold import run as gold_run

with DAG(
    dag_id="brewery_full_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["enterprise"]
) as dag:

    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=bronze_run,
        retries=3
    )

    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=silver_run,
        retries=2
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=gold_run,
        retries=2
    )

    bronze_task >> silver_task >> gold_task