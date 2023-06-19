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

dag = DAG('etl_dag', default_args=default_args,schedule_interval='30 11 * * *')

task_extract = BashOperator(
    task_id='api_extract',
    bash_command='python ./scripts/api_extract.py',
    dag=dag,
    cwd='/'
)

task_ingest = SparkSubmitOperator(
    task_id='data_ingest',
    application='./scripts/data_ingest.py',
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

task_load_awardings = SparkSubmitOperator(
    task_id='load_awardings',
    application='./scripts/load_awardings.py',
    conn_id='spark_default',
    dag=dag
)

task_load_gildings = SparkSubmitOperator(
    task_id='load_gildings',
    application='./scripts/load_gildings.py',
    conn_id='spark_default',
    dag=dag
)

task_load_popular = SparkSubmitOperator(
    task_id='load_popular',
    application='./scripts/load_popular.py',
    conn_id='spark_default',
    dag=dag
)

task_extract >> task_ingest >> task_transform_awardings >> task_load_awardings
task_extract >> task_ingest >> task_transform_gildings >> task_load_gildings
task_extract >> task_ingest >> task_transform_popular >> task_load_popular