import requests
from vars import *
import boto3


def loadConfigs(builder):
    builder.master('local[1]') \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
           .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
           .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
           .config("spark.jars.packages", "/usr/local/spark/jars/delta-core_2.12-2.4.0.jar") \
           .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
           .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
           .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
           .config("spark.hadoop.fs.s3a.path.style.access", "true") \
           .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
           .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return builder


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
