from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType
from delta import *
from pyspark.sql.functions import current_date
from pyspark.sql.functions import month, year
from pyspark.sql.types import LongType

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

output_file = "gildings"

df_raw = spark.read.format("delta").load(f"s3a://{minio_bucket}/bronze/popular/popular_{today}")

df_gildings = df_raw.select("id", "author", "gildings", "is_video", "score", "subreddit_id", "subreddit") \
                      .withColumnRenamed("id", "post_id")

df_gildings = df_gildings.select("*", "gildings.*")

if "gid_1" not in df_gildings.columns:
    df_gildings = df_gildings.withColumn("gid_1", lit(None).cast(IntegerType()))

df_renamed = df_gildings.withColumnRenamed("gid_1", "gild_silver") \
                        .withColumnRenamed("gid_2", "gild_gold") \
                        .withColumnRenamed("gid_3", "gild_platinum")

df_clean = df_renamed.dropDuplicates()

df_clean = df_clean.withColumn("year", year(current_date())) \
                   .withColumn("month", month(current_date())) \
                   .withColumn("created_date", current_date())

# df_clean = df_clean.select(gildings)

columns_to_fill = ["gild_silver","gild_gold", "gild_platinum", "score"]
df_final = df_clean.na.fill(0, subset=columns_to_fill)

df_final = df_final.drop("gildings")

df_final = df_final.withColumn("gild_silver", df_final["gild_silver"].cast(LongType()))

df_final = df_final.orderBy('gild_gold', ascending=False)

df_final.write.format("delta") \
        .partitionBy("created_date") \
        .mode("append") \
        .save(f"s3a://{minio_bucket}/silver/{output_file}")