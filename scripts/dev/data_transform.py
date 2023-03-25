from pyspark.sql import SparkSession
from vars import *
from datetime import date
from pyspark.sql.functions import lit
from functions import flatten_json, loadConfigs

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

try:
    today = date.today().strftime('%Y%m%d')
    dateid = int(today)

    json_df = spark.read.option("header", "true") \
        .json(f"s3a://{minio_bucket}/raw/popular_{today}.json")

    json_df = json_df.select("data")

    flatten_df = flatten_json(json_df, "data")
    flatten_df = flatten_df.withColumn("dateid", lit(dateid))

    flatten_df.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/processed/popular_{today}.parquet")
except Exception as e:
    print(e)