from vars import *
import json
import boto3
import requests
from datetime import date

# Use HTTPBasicAuth to authenticate the request with the Reddit API
auth = requests.auth.HTTPBasicAuth(personal_use_script, secret_token)

# Set up the data for the request to get an access token
data = {'grant_type': 'password',
        'username': username,
        'password': password}

# Set the headers for the request
headers = {'User-Agent': 'aws-de/0.0.1'}

# Send a POST request to get an access token
res = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

# Save the access token
TOKEN = res.json()['access_token']

# Add the access token to the headers for future requests
headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}

# Send a GET request to get the hot posts in the "popular" subreddit
res = requests.get("https://oauth.reddit.com/r/popular",
                   headers=headers)

# Get the JSON data from the response
data = res.json()

# Convert the JSON data to a string
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