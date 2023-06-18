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

input_file = "awardings"
output_folder = "awardings"

df = spark.read.format("delta") \
    .load(f"s3a://{minio_bucket}/silver/{input_file}")

# Author data
output_file = "top_authors"

author_df = df.groupBy("year", "month", "author","award_sub_type","award_type").agg(
    F.first("score").alias("score"),
    F.sum("coin_price").alias("total_coin_price"),
    F.sum("coin_reward").alias("total_coin_reward"),
    F.sum("count").alias("total_coin_count")
)

author_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")

# Subreddit data
output_file = "top_subreddits"

subreddit_df = df.groupBy("year", "month", "subreddit","award_sub_type","award_type").agg(
    F.first("score").alias("score"),
    F.sum("coin_price").alias("total_coin_price"),
    F.sum("coin_reward").alias("total_coin_reward"),
    F.sum("count").alias("total_coin_count")
)

subreddit_df.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")