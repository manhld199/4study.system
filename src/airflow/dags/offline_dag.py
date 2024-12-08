# Set environment
import sys, os
sys.path.append(os.path.abspath("."))

# Import libs
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import custom modules
from process.preprocess import preprocess_csv
from process.build_graph import build_graph

default_args = {
	'owner':'Group1',
	'start_date': datetime(2024, 12, 7, 10, 0),
	'retries': 5,
  'retry_delay': timedelta(minutes=10),
}

with DAG(
	dag_id='offline_dag',
	default_args=default_args,
	schedule_interval='@daily'
) as dag:
  data_preprocessing=PythonOperator(
    task_id='data_processing',
    python_callable=preprocess_csv,
    op_kwargs={
      'input_path': "/opt/airflow/data/raw",
      'output_path': "/opt/airflow/data/preprocessed",
    }
  )

  graph_building=PythonOperator(
    task_id='graph_building',
    python_callable=build_graph,
    op_kwargs={
      'input_path': "/opt/airflow/data/raw",
      'output_path': "/opt/airflow/data/graph",
    }
  )

  data_preprocessing >> graph_building