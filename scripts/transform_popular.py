from pyspark.sql import SparkSession
from vars import *
from datetime import date
from functions import loadConfigs
from delta import *
from pyspark.sql.functions import current_date
from pyspark.sql.functions import month, year
from pyspark.sql.functions import col
from columns import popular
from pyspark.sql.functions import col, when


builder = loadConfigs(SparkSession.builder)
spark = configure_spark_with_delta_pip(builder).getOrCreate()

today = date.today().strftime('%Y%m%d')
# today = 20230326

output_file = "popular"

df_raw = spark.read.format("delta").load(f"s3a://{minio_bucket}/bronze/popular/popular_{today}")

df = df_raw.withColumnRenamed("id", "post_id")

df = df.withColumn("selftext", when(col("selftext") == "", None).otherwise(col("selftext")))

df_extend = df.withColumn("year", year(current_date())) \
                   .withColumn("month", month(current_date())) \
                   .withColumn("created_date", current_date())

df_final = df_extend.select(popular)
df_final = df_final.dropDuplicates()

df_final.write.format("delta") \
        .partitionBy("created_date") \
        .mode("append") \
        .save(f"s3a://{minio_bucket}/silver/{output_file}")