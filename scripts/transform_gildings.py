from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from columns import gildings
from pyspark.sql.types import IntegerType
from delta import *
from pyspark.sql.functions import current_date, date_format
from pyspark.sql import functions as F

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

df_raw = spark.read.format("delta").load(f"s3a://{minio_bucket}/bronze/popular/popular_{today}")

df_gildings = df_raw.select("id", "author", "gilded", "gildings", "is_video", "score", "subreddit_id", "subreddit", "total_awards_received") \
                      .withColumnRenamed("id", "post_id")

df_gildings = df_gildings.select("*", "gildings.*")

if "gid_1" not in df_gildings.columns:
    df_gildings = df_gildings.withColumn("gid_1", lit(None).cast(IntegerType()))

df_renamed = df_gildings.withColumnRenamed("gid_1", "gild_silver") \
                        .withColumnRenamed("gid_2", "gild_gold") \
                        .withColumnRenamed("gid_3", "gild_platinum")

df_clean = df_renamed.dropDuplicates()

df_clean = df_clean.withColumn("dateid", lit(today))

df_clean = df_clean.select(gildings)

columns_to_fill = ["gild_silver","gild_gold", "gild_platinum", "score"]
df_clean = df_clean.na.fill(0, subset=columns_to_fill)

# Author data
output_folder = "gildings"
output_file = "author_gildings"

author_df = df_clean.groupBy("author","subreddit_id","subreddit").agg(
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

author_df.write.format("delta").partitionBy("updated_at").mode("append").save(f"s3a://{minio_bucket}/silver/{output_folder}/{output_file}")

# Post data
output_file = "post_gildings"

post_df = df_clean.groupBy("post_id","subreddit_id","subreddit").agg(
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

post_df.write.format("delta").partitionBy("updated_at").mode("append").save(f"s3a://{minio_bucket}/silver/{output_folder}/{output_file}")

# Subreddit data
output_file = "subreddit_gildings"

subreddit_df = df_clean.groupBy("subreddit_id","subreddit").agg(
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

subreddit_df.write.format("delta").partitionBy("updated_at").mode("append").save(f"s3a://{minio_bucket}/silver/{output_folder}/{output_file}")