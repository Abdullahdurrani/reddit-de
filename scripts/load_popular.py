from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
import os

spark_home = os.environ.get('SPARK_HOME')

spark = SparkSession.builder \
    .master('local[1]') \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

today = date.today().strftime('%Y%m%d')
# today = 20230326

input_folder = "popular"
input_file = "popular"
table_name = "staging.popular"
mode = "append"

df = spark.read.option("header", "true") \
    .parquet(f"s3a://{minio_bucket}/silver/{input_folder}/{input_file}_{today}")

