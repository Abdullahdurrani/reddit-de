# Reddit Data Engineering Project

>This project is intended to create an ETL pipeline using Reddit'API. Currently this project is under-development.

Currently implemented ingestion process retrieves the top posts from the "popular" subreddit on Reddit's API and stores the resulting JSON data in a MinIO bucket.

## Prerequisites
Before running this script, you must have:

- A Reddit account
- A MinIO instance with credentials and endpoint URL

## Installation
To install the required Python packages, run the following command:

```
pip install -r requirements.txt
```

## Usage
To run the data ingestion script, use the following command:

```
python scripts/data_ingestion.py
```

The script will retrieve the top posts from the "popular" subreddit and store the resulting JSON data in a MinIO bucket.

## Configuration
The script requires the following environment variables to be set:

* **PERSONAL_USE_SCRIPT**: Your Reddit developer account's personal use script key
* **SECRET_TOKEN**: Your Reddit developer account's secret token
* **USERNAME**: Your Reddit account's username
* **PASSWORD**: Your Reddit account's password
* **MINIO_ENDPOINT**: The URL of your MinIO instance
* **MINIO_ACCESS_KEY**: Your MinIO instance's access key
* **MINIO_SECRET_KEY**: Your MinIO instance's secret key
* **MINIO_BUCKET**: The name of the MinIO bucket to store the JSON data in
