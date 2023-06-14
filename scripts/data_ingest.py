from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from delta import *

builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

output_folder = "popular"
output_file = "popular"

today = date.today().strftime('%Y%m%d')
# today = 20230326

df_raw = spark.read.option("header", "true") \
    .json(f"s3a://{minio_bucket}/landing/popular_{today}.json")

df_raw = df_raw.select(explode(df_raw.data.children.data).alias("data"))
df_raw = df_raw.select("data.*")

df_final = df_raw.withColumn("dateid", lit(today))

df_final.write.format("delta").mode("overwrite").save(f"s3a://{minio_bucket}/bronze/{output_folder}/{output_file}_{today}")