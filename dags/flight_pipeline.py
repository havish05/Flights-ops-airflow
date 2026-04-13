import sys
from airflow import DAG
from pathlib import Path
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

AIRFLOW_HOME = Path("/opt/airflow")

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import run_bronze_ingestion

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id= "flight_ops_medallion_pipeline",
    start_date= datetime(2026, 4, 10),
    default_args= default_args,
    schedule= "*/30 * * * *",
    catchup= False
) as dag: 
        
        bronze = PythonOperator(
            task_id= "bronze_ingestion",
            python_callable= run_bronze_ingestion
        )
