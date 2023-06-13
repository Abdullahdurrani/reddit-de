from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date, date_format

spark_home = os.environ.get('SPARK_HOME')

spark = SparkSession.builder \
    .master('local[1]') \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

today = date.today().strftime('%Y%m%d')
# today = 20230326

input_folder = "gildings"
input_file = "gildings"

# Author data

output_folder = "author_gildings"
output_file = "author_gildings"

df = spark.read.option("header", "true") \
    .parquet(f"s3a://{minio_bucket}/silver/{input_folder}/{input_file}_{today}")

author_df = df.groupBy("author","subreddit_id","subreddit").agg(
    F.sum("gild_silver").alias("total_gild_silver"),
    F.sum("gild_gold").alias("total_gild_gold"),
    F.sum("gild_platinum").alias("total_gild_platinum"),
    F.sum("score").alias("total_score")
)

author_df = author_df.withColumn(
    'total_gildings',
    F.col('total_gild_silver') + F.col('total_gild_gold') + F.col('total_gild_platinum')
).orderBy('total_gildings',ascending=False)

author_df = author_df.withColumn("updated_at", date_format(current_date(), "yyyyMMdd"))

author_df.write.format("delta").partitionBy("updated_at").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")

# Post data
output_folder = "post_gildings"
output_file = "post_gildings"

post_df = df.groupBy("post_id","subreddit_id","subreddit").agg(
    F.sum("gild_silver").alias("total_gild_silver"),
    F.sum("gild_gold").alias("total_gild_gold"),
    F.sum("gild_platinum").alias("total_gild_platinum"),
    F.sum("score").alias("total_score")
)

post_df = post_df.withColumn(
    'total_gildings',
    F.col('total_gild_silver') + F.col('total_gild_gold') + F.col('total_gild_platinum')
).orderBy('total_gildings',ascending=False)

post_df = post_df.withColumn("updated_at", date_format(current_date(), "yyyyMMdd"))

post_df.write.format("delta").partitionBy("updated_at").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")

# Subreddit data
output_folder = "subreddit_gildings"
output_file = "subreddit_gildings"

subreddit_df = df.groupBy("subreddit_id","subreddit").agg(
    F.sum("gild_silver").alias("total_gild_silver"),
    F.sum("gild_gold").alias("total_gild_gold"),
    F.sum("gild_platinum").alias("total_gild_platinum"),
    F.sum("score").alias("total_score")
)

subreddit_df = subreddit_df.withColumn(
    'total_gildings',
    F.col('total_gild_silver') + F.col('total_gild_gold') + F.col('total_gild_platinum')
).orderBy('total_gildings',ascending=False)

subreddit_df = subreddit_df.withColumn("updated_at", date_format(current_date(), "yyyyMMdd"))

subreddit_df.write.format("delta").partitionBy("updated_at").mode("overwrite").save(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}")