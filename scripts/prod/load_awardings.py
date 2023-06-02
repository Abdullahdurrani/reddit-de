from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
import os

spark_home = os.environ.get('SPARK_HOME')

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars", f"{spark_home}/jars/postgresql-42.2.5.jar") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

today = date.today().strftime('%Y%m%d')
# today = 20230326

input_folder = "awardings"
input_file = "awardings"
table_name = "staging.awardings"
mode = "append"

df = spark.read.option("header", "true") \
    .parquet(f"s3a://{minio_bucket}/processed/{input_folder}/{input_file}_{today}")

columns = df.columns
print(columns)

properties = {"user": postgres_user, "password": postgres_pass}
df.write.jdbc(url=postgres_url, table=table_name, mode=mode, properties=properties)