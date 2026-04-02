from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define DAG
with DAG(
    dag_id="retail_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger for now
    catchup=False,
) as dag:

    # Task 1: Ingest CSV
    ingest = BashOperator(
        task_id="ingest_data",
        bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/ingest_csv.py"
    )

    # Task 2: Transform Data
    transform = BashOperator(
        task_id="transform_data",
        bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/transform_data.py"
    )

    # Task 3: Merge Data
    merge = BashOperator(
        task_id="merge_data",
        bash_command="python /opt/airflow/scripts/merge_data.py"
    )

    # Task Dependencies
    ingest >> transform >> merge
