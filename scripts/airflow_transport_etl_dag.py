from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Path to your project root (adjust if you move the repo)
PROJECT_ROOT = r"C:\Users\varun\OneDrive\Desktop\Hackathon_3_MidTerm"
SCRIPTS_DIR = os.path.join(PROJECT_ROOT, "scripts")

# Ensure Python can import from the scripts folder
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

# Import the existing run_pipeline() function from your ETL script
from etl_pipeline import run_pipeline


default_args = {
    "owner": "transport_etl_student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="transport_etl_dag",
    default_args=default_args,
    description="Mid-term hackathon ETL: public transport + traffic (India)",
    schedule_interval="@daily",  # could also be "@hourly"
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["midterm", "transport", "etl"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_transport_etl",
        python_callable=run_pipeline,
    )

    run_etl
