from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.jars", "/jars/postgresql-42.2.5.jar") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

today = date.today().strftime('%Y%m%d')
today = 20230326

output_folder = "gildings"
output_file = "gildings"

df_raw = spark.read.option("header", "true") \
    .json(f"s3a://{minio_bucket}/raw/popular_{today}.json")

df_raw = df_raw.select(explode(df_raw.data.children.data).alias("data"))
df_raw = df_raw.select("data.*")

df_gildings = df_raw.select("id", "author", "gilded", "gildings") \
                      .withColumnRenamed("id", "post_id")

df_gildings = df_gildings.select("*", "gildings.*")

df_renamed = df_gildings.withColumnRenamed("gid_1", "gild_silver") \
                        .withColumnRenamed("gid_2", "gild_gold") \
                        .withColumnRenamed("gid_3", "gild_platinum")

df_final = df_renamed.dropDuplicates()

df_final = df_final.withColumn("dateid", lit(today))

df_final.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/processed/{output_folder}/{output_file}_{today}")