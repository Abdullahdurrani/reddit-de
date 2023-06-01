from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 31),
    'retries': 0,
}

dag = DAG('etl_dag', default_args=default_args)

task_ingest = BashOperator(
    task_id='data_ingestion',
    bash_command='python ./scripts/prod/data_ingestion.py',
    dag=dag,
    cwd='/'
)

task_transform_author = SparkSubmitOperator(
    task_id='transform_author',
    application='./scripts/prod/transform_author_flair.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_awardings = SparkSubmitOperator(
    task_id='transform_awardings',
    application='./scripts/prod/transform_awardings.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_gildings = SparkSubmitOperator(
    task_id='transform_gildings',
    application='./scripts/prod/transform_gildings.py',
    conn_id='spark_default',
    dag=dag
)

task_transform_popular = SparkSubmitOperator(
    task_id='transform_popular',
    application='./scripts/prod/transform_popular.py',
    conn_id='spark_default',
    dag=dag
)

task_load_popular = SparkSubmitOperator(
    task_id='load_popular',
    application='./scripts/prod/load_popular.py',
    conn_id='spark_default',
    dag=dag
)

task_ingest >> [task_transform_author, task_transform_awardings, task_transform_gildings, task_transform_popular] >> task_load_popular