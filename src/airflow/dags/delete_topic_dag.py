# Set environment
import sys, os
sys.path.append(os.path.abspath("."))

# Import libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import custom modules
from process.consume_kafka import delete_topic

default_args = {
	'owner':'Group1',
	'start_date': datetime(2024, 12, 7, 10, 0),
	'retries': 5,
  'retry_delay': timedelta(minutes=10),
}

with DAG(
	dag_id='delete_kafka_topic_dag22',
	default_args=default_args,
	# schedule_interval='@daily'
) as dag:
  data_deleting=PythonOperator(
    task_id='kafka_topic_deleting',
    python_callable=delete_topic,
  )

  data_deleting