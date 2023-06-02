from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

print("Starting")
default_args = {
    'owner': 'dw',
    'start_date': datetime(2021, 5, 9),
    "retries": 0,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG('my_dag', description = 'spark test', catchup = False, schedule_interval = "@hourly", default_args = default_args)

jars = 'opt/airflow/dags/'

task_load_popular = SparkSubmitOperator(
    task_id='load_popular',
    application='./scripts/prod/load_popular.py',
    conn_id='spark_default',
    dag=dag
)