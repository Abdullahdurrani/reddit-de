from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from columns import gildings
from pyspark.sql.types import IntegerType
from delta import *

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

output_folder = "gildings"
output_file = "gildings"

df_raw = spark.read.option("header", "true") \
    .json(f"s3a://{minio_bucket}/bronze/popular_{today}.json")

df_raw = df_raw.select(explode(df_raw.data.children.data).alias("data"))
df_raw = df_raw.select("data.*")

df_gildings = df_raw.select("id", "author", "gilded", "gildings", "is_video", "score", "subreddit_id", "subreddit", "total_awards_received") \
                      .withColumnRenamed("id", "post_id")

df_gildings = df_gildings.select("*", "gildings.*")

if "gid_1" not in df_gildings.columns:
    df_gildings = df_gildings.withColumn("gid_1", lit(None).cast(IntegerType()))

df_renamed = df_gildings.withColumnRenamed("gid_1", "gild_silver") \
                        .withColumnRenamed("gid_2", "gild_gold") \
                        .withColumnRenamed("gid_3", "gild_platinum")

df_final = df_renamed.dropDuplicates()

df_final = df_final.withColumn("dateid", lit(today))

df_final = df_final.select(gildings)

df_final.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/silver/{output_folder}/{output_file}_{today}")