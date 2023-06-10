from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

print("Starting")
default_args = {
    'owner': 'dw',
    'start_date': datetime(2021, 5, 9),
    "retries": 0,
    "retry_delay": timedelta(minutes = 1)
}

dag = DAG('my_dag', description = 'spark test', catchup = False, schedule_interval = "@hourly", default_args = default_args)

jars = 'opt/airflow/dags/'

task_transform_author = SparkSubmitOperator(
    task_id='transform_author',
    application='./scripts/transform_author_flair.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_awardings = SparkSubmitOperator(
    task_id='transform_awardings',
    application='./scripts/transform_awardings.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_gildings = SparkSubmitOperator(
    task_id='transform_gildings',
    application='./scripts/transform_gildings.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_popular = SparkSubmitOperator(
    task_id='transform_popular',
    application='./scripts/transform_popular.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_gildings >> task_transform_author >> task_transform_awardings >> task_transform_popular