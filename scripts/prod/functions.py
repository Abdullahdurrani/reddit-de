import requests
from vars import *
import boto3

def loadConfigs(sparkContext):
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", minio_access_key)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", minio_secret_key)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_endpoint)
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sparkContext._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

def reddit_connection():
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

    return headers

def minio_connection():
    # Create a boto3 client for MinIO
    minio_client = boto3.client('s3',
                            endpoint_url=minio_endpoint,
                            aws_access_key_id=minio_access_key,
                            aws_secret_access_key=minio_secret_key,
                            region_name=minio_region)
    return minio_client