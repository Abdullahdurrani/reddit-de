from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from delta import *
from pyspark.sql.functions import current_date
from pyspark.sql.functions import month, year
from columns import awardings
from pyspark.sql.functions import col, explode

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

output_file = "awardings"

df_raw = spark.read.format("delta").load(f"s3a://{minio_bucket}/bronze/popular/popular_{today}")

df_awardings = df_raw.select(col("id").alias("post_id"),
                             col("author"),
                             col("all_awardings"),
                             col("score"),
                             col("subreddit_id").alias("sub_id"),
                             col("subreddit").alias("sub"))

df_awardings_exploded = df_awardings.select("*", explode("all_awardings").alias("awardings"))
df_awardings = df_awardings_exploded.select("*", "awardings.*")
df_awardings_cleaned = df_awardings.drop("all_awardings","awardings","subreddit_id","subreddit")

df_awardings_cleaned = df_awardings_cleaned.withColumnRenamed("sub_id", "subreddit_id").withColumnRenamed("sub", "subreddit")
df_dedup = df_awardings_cleaned.dropDuplicates()

df_final = df_dedup.withColumn("year", year(current_date())) \
                   .withColumn("month", month(current_date())) \
                   .withColumn("created_date", current_date())

df_final = df_final.select(awardings)

df_final.write.format("delta") \
        .partitionBy("created_date") \
        .mode("append") \
        .save(f"s3a://{minio_bucket}/silver/{output_file}")