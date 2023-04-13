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

output_folder = "popular"
output_file = "popular"

df_raw = spark.read.option("header", "true") \
    .json(f"s3a://{minio_bucket}/raw/popular_{today}.json")

df_raw = df_raw.select(explode(df_raw.data.children.data).alias("data"))
df_raw = df_raw.select("data.*")

columns_list = df_raw.columns

flair_list = [s for s in columns_list if "_flair" in s or "mod_" in s or "url" in s or "remov" in s]

columns_to_drop = flair_list + ["all_awardings","author_flair_richtext","author_is_blocked","gildings","link_flair_richtext","media","preview",
                                "tournament_data","allow_live_comments","approved_by","banned_at_utc","banned_by","contest_mode",
                                "thumbnail_height","thumbnail_width","user_reports","treatment_tags","content_categories","awarders"]

df_cleaned = df_raw.drop(*[col for col in df_raw.columns if any(s in col for s in columns_to_drop)])

df_final = df_cleaned.dropDuplicates()
df_final = df_final.withColumn("dateid", lit(today))

df_final.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/processed/{output_folder}/{output_file}_{today}")