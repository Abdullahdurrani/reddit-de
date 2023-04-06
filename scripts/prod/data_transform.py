from pyspark.sql import SparkSession
from vars import *
from datetime import date
from pyspark.sql.functions import lit
from functions import flatten_json, loadConfigs

spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()
loadConfigs(spark.sparkContext)

try:
    today = date.today().strftime('%Y%m%d')
    dateid = int(today)

    json_df = spark.read.option("header", "true") \
        .json(f"s3a://{minio_bucket}/raw/popular_{today}.json")

    json_df = json_df.select("data")

    flatten_df = flatten_json(json_df, "data")

    columns = ['data>children>data>author','data>children>data>author_cakeday','data>children>data>author_flair_text',
           'data>children>data>author_fullname','data>children>data>author_is_blocked',
           'data>children>data>can_gild','data>children>data>category','data>children>data>content_categories',
           'data>children>data>created_utc','data>children>data>domain','data>children>data>downs','data>children>data>edited',
           'data>children>data>gilded','data>children>data>id','data>children>data>is_original_content', 
           'data>children>data>is_robot_indexable','data>children>data>is_self',
           'data>children>data>is_video','data>children>data>link_flair_text','data>children>data>name','data>children>data>num_comments',
           'data>children>data>num_crossposts','data>children>data>over_18','data>children>data>parent_whitelist_status',
           'data>children>data>permalink','data>children>data>post_hint','data>children>data>pwls','data>children>data>score',
           'data>children>data>selftext','data>children>data>selftext_html','data>children>data>spoiler','data>children>data>subreddit',
           'data>children>data>subreddit_id','data>children>data>subreddit_name_prefixed','data>children>data>subreddit_subscribers',
           'data>children>data>subreddit_type','data>children>data>suggested_sort','data>children>data>thumbnail',
           'data>children>data>title','data>children>data>top_awarded_type','data>children>data>total_awards_received',
           'data>children>data>ups','data>children>data>upvote_ratio','data>children>data>url','data>children>data>url_overridden_by_dest',
           'data>children>data>view_count','data>children>data>wls','data>children>data>all_awardings>award_sub_type',
           'data>children>data>all_awardings>award_type','data>children>data>all_awardings>coin_price',
           'data>children>data>all_awardings>coin_reward','data>children>data>all_awardings>count',
           'data>children>data>all_awardings>days_of_drip_extension','data>children>data>all_awardings>days_of_premium',
           'data>children>data>all_awardings>description','data>children>data>all_awardings>end_date',
           'data>children>data>all_awardings>giver_coin_reward','data>children>data>all_awardings>id',
           'data>children>data>all_awardings>name','data>children>data>all_awardings>penny_price',
           'data>children>data>all_awardings>start_date','data>children>data>all_awardings>subreddit_coin_reward',
           'data>children>data>all_awardings>subreddit_id','data>children>data>all_awardings>tiers_by_required_awardings',
           'data>children>data>author_flair_richtext>a','data>children>data>author_flair_richtext>e',
           'data>children>data>author_flair_richtext>t','data>children>data>author_flair_richtext>u',
           'data>children>data>gildings>gid_1','data>children>data>gildings>gid_2','data>children>data>gildings>gid_3',
           'data>children>data>link_flair_richtext>a','data>children>data>link_flair_richtext>e',
           'data>children>data>link_flair_richtext>t', 'data>children>data>link_flair_richtext>u',
           'data>children>data>media>reddit_video>bitrate_kbps','data>children>data>media>reddit_video>duration',
           'data>children>data>media>reddit_video>height','data>children>data>media>reddit_video>is_gif',
           'data>children>data>media>reddit_video>width','data>children>data>preview>images>id','data>children>data>secure_media>reddit_video>bitrate_kbps',
           'data>children>data>secure_media>reddit_video>duration','data>children>data>secure_media>reddit_video>height','data>children>data>secure_media>reddit_video>is_gif',
           'data>children>data>secure_media>reddit_video>width','data>children>data>preview>images>resolutions>height','data>children>data>preview>images>resolutions>url',
           'data>children>data>preview>images>resolutions>width','data>children>data>preview>images>source>height','data>children>data>preview>images>source>url',
           'data>children>data>preview>images>source>width','data>children>data>tournament_data>predictions>options>id','data>children>data>tournament_data>predictions>options>image_url',
           'data>children>data>tournament_data>predictions>options>text','data>children>data>tournament_data>predictions>options>total_amount',
           'data>children>data>tournament_data>predictions>options>user_amount','data>children>data>tournament_data>predictions>options>vote_count',
           'data>children>data>preview>images>variants>obfuscated>resolutions>height','data>children>data>preview>images>variants>obfuscated>resolutions>url',
           'data>children>data>preview>images>variants>obfuscated>resolutions>width','data>children>data>preview>images>variants>obfuscated>source>height',
           'data>children>data>preview>images>variants>obfuscated>source>url','data>children>data>preview>images>variants>obfuscated>source>width']

    df_data = flatten_df.select(columns)

    df_data_dedup = df_data.dropDuplicates()

    new_columns = [c.replace('>', '_') for c in df_data_dedup.columns]
    df_renamed = df_data_dedup.toDF(*new_columns)

    new_columns = [c.replace('data_children_data_', '') for c in df_renamed.columns]
    df_renamed = df_renamed.toDF(*new_columns)
    
    df_final = df_renamed.withColumn("dateid", lit(dateid))

    df_final.write.mode("overwrite").parquet(f"s3a://{minio_bucket}/processed/popular_{today}.parquet")
except Exception as e:
    print(e)