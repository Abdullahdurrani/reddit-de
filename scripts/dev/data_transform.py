from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import flatten_json

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

try:
    file_date = date.today().strftime('%Y%m%d')

    json_df = spark.read.option("header", "true") \
        .json(f"s3a://{minio_bucket}/raw/popular_{file_date}.json")

    json_df = json_df.select("data")

    flatten_df = flatten_json(json_df, "data")

    flatten_df.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/processed/popular_{file_date}.parquet")
except Exception as e:
    print(e)