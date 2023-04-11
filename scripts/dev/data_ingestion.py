from vars import *
import json
import boto3
import requests
from datetime import date
from functions import reddit_connection

headers = reddit_connection()

# Send a GET request to get the hot posts in the "popular" subreddit
res = requests.get("https://oauth.reddit.com/r/popular",
                   headers=headers)

# Get the JSON data from the response
data = res.json()

# Convert the JSON data to a string to not get TypeError while writing
json_data = json.dumps(data)

# Create a boto3 client for MinIO
s3_client = boto3.client('s3',
                         endpoint_url=minio_endpoint,
                         aws_access_key_id=minio_access_key,
                         aws_secret_access_key=minio_secret_key,
                         region_name=minio_region)

file_date = date.today().strftime('%Y%m%d')

# Upload the JSON data to the MinIO bucket
s3_client.put_object(Bucket=minio_bucket,
                     Key=f'raw/popular_{file_date}.json',
                     Body=json_data.encode('utf-8'))