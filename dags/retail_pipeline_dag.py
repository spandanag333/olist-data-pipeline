# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "retries": 1,
# }

# with DAG(
#     dag_id="retail_pipeline",
#     default_args=default_args,
#     start_date=datetime(2026, 4, 4),
#     schedule_interval="@daily",  # runs once per day
#     catchup=False,
# ) as dag:

#     ingest = BashOperator(
#         task_id="ingest_data",
#         bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/ingest_csv.py"
#     )

#     transform = BashOperator(
#         task_id="transform_data",
#         bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/transform_data.py"
#     )

#     merge = BashOperator(
#         task_id="merge_data",
#         bash_command="PYTHONPATH=/opt/airflow python /opt/airflow/scripts/merge_data.py"
#     )

#     validate = BashOperator(
#         task_id="validate_output",
#         bash_command="test -f /opt/airflow/data/merged/fact_orders_star.csv"
#     )

#     ingest >> transform >> merge >> validate


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys


sys.path.append("/opt/airflow/scripts")  # so Airflow can find your scripts

# Import your pipeline functions directly
from ingest_csv import main as ingest_main
from transform_data import main as transform_main
from merge_data import main as merge_main

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="retail_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",  # runs once per day
    catchup=False,
) as dag:

    ingest = PythonOperator(
    task_id="ingest_data",
    python_callable=ingest_main
)

transform = PythonOperator(
    task_id="transform_data",
    python_callable=transform_main
)

merge = PythonOperator(
    task_id="merge_data",
    python_callable=merge_main
)

ingest >> transform >> merge



