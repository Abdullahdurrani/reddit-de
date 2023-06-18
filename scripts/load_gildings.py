from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql import functions as F
from delta import *

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

input_file = "gildings"
output_folder = "gildings"

df = spark.read.format("delta") \
    .load(f"s3a://{minio_bucket}/silver/{input_file}")

# Author data
output_file = "top_authors"

author_df = df.groupBy("year", "month", "author").agg(
    F.sum("gild_silver").alias("total_silver_gilds"),
    F.sum("gild_gold").alias("total_gold_gilds"),
    F.sum("gild_platinum").alias("total_platinum_gilds"),
    (F.sum("gild_silver") + F.sum("gild_gold") + F.sum("gild_platinum")).alias("total_gilds")
)

author_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")

# Subreddit data
output_file = "top_subreddits"

subreddit_df = df.groupBy("year", "month", "subreddit").agg(
    F.sum("gild_silver").alias("total_silver_gilds"),
    F.sum("gild_gold").alias("total_gold_gilds"),
    F.sum("gild_platinum").alias("total_platinum_gilds"),
    (F.sum("gild_silver") + F.sum("gild_gold") + F.sum("gild_platinum")).alias("total_gilds")
)
subreddit_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")