import requests
from vars import *

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
res = requests.get("https://oauth.reddit.com/r/popular/hot",
                   headers=headers)

print(res.json())