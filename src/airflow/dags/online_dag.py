# Set environment
import sys, os
sys.path.append(os.path.abspath("."))

# Import libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import custom modules
from process.produce_kafka import produce_data

default_args = {
	'owner':'Group1',
	'start_date': datetime(2024, 12, 7, 10, 0),
	'retries': 5,
  'retry_delay': timedelta(minutes=10),
}

with DAG(
	dag_id='online_dag',
	default_args=default_args,
	schedule_interval='@daily'
) as dag:
  data_collecting=PythonOperator(
    task_id='data_collecting',
    python_callable=produce_data,
  )

  data_collecting