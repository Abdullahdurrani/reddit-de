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

input_file = "popular"
output_folder = "popular"

df = spark.read.format("delta") \
    .load(f"s3a://{minio_bucket}/silver/{input_file}")

# Author data
output_file = "top_authors"

author_df = df.groupBy("year", "month", "author").agg(
    F.sum("downs").alias("total_downs"),
    F.sum("gilded").alias("total_gilded"),
    F.sum("num_crossposts").alias("total_num_crossposts"),
    F.sum("score").alias("total_score"),
    F.sum("total_awards_received").alias("total_awards_received"),
    F.sum("ups").alias("total_ups")
)

author_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")

# Subreddit data
output_file = "top_subreddits"

subreddit_df = df.groupBy("year", "month", "subreddit").agg(
    F.sum("downs").alias("total_downs"),
    F.sum("gilded").alias("total_gilded"),
    F.sum("num_crossposts").alias("total_num_crossposts"),
    F.sum("score").alias("total_score"),
    F.sum("total_awards_received").alias("total_awards_received"),
    F.sum("ups").alias("total_ups")
)

subreddit_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")