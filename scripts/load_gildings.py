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
output_folder = "author_gildings"
mode = "append"

df = spark.read.option("header", "true") \
    .parquet(f"s3a://{minio_bucket}/silver/{input_folder}/{input_file}_{today}")

author_df = df.groupBy("author").agg(
    F.sum("gild_silver").alias("total_gild_silver"),
    F.sum("gild_gold").alias("total_gild_gold"),
    F.sum("gild_platinum").alias("total_gild_platinum")
)

author_df = author_df.withColumn(
    'total_gildings',
    F.col('total_gild_silver') + F.col('total_gild_gold') + F.col('total_gild_platinum')
).orderBy('total_gildings',ascending=False)

author_df = author_df.withColumn("updated_at", date_format(current_date(), "yyyyMMdd"))

author_df.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/gold/{output_folder}/{output_file}_{today}")

post_df = df.groupBy("post_id").agg(
    F.sum("gild_silver").alias("total_gild_silver"),
    F.sum("gild_gold").alias("total_gild_gold"),
    F.sum("gild_platinum").alias("total_gild_platinum")
)

post_df = post_df.withColumn(
    'total_gildings',
    F.col('total_gild_silver') + F.col('total_gild_gold') + F.col('total_gild_platinum')
).orderBy('total_gildings',ascending=False)

post_df = post_df.withColumn("updated_at", date_format(current_date(), "yyyyMMdd"))