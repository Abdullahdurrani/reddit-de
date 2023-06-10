from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from pyspark.sql.functions import lit
from pyspark.sql.functions import explode
from columns import author_flair

spark = SparkSession.builder \
    .master('local[1]') \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

today = date.today().strftime('%Y%m%d')
# today = 20230326

output_folder = "author_flair"
output_file = "author_flair"

df_raw = spark.read.option("header", "true") \
    .json(f"s3a://{minio_bucket}/bronze/popular_{today}.json")

df_raw = df_raw.select(explode(df_raw.data.children.data).alias("data"))
df_raw = df_raw.select("data.*")

df_author_flair_richtext = df_raw.select("id","author","author_flair_richtext",
                                         "author_flair_template_id","author_flair_template_id",
                                         "author_flair_text","author_flair_type") \
                                  .withColumnRenamed("id", "post_id")

df_author_flair_exploded = df_author_flair_richtext.select("post_id","author",
                                                           "author_flair_template_id",
                                                           "author_flair_text","author_flair_type",
                                                           explode("author_flair_richtext").alias("author_flair_richtext"))

df_author_flair_cleaned = df_author_flair_exploded.select("*", "author_flair_richtext.*")
df_author_flair_cleaned = df_author_flair_cleaned.drop("author_flair_richtext", "u")

df_author_flair_renamed = df_author_flair_cleaned.withColumnRenamed("a", "additional_attributes") \
                                                                   .withColumnRenamed("e", "type") \
                                                                   .withColumnRenamed("t", "text")

df_final = df_author_flair_renamed.dropDuplicates()

df_final = df_final.withColumn("dateid", lit(today))

df_final = df_final.select(author_flair)

df_final.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/silver/{output_folder}/{output_file}_{today}")