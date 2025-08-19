import json
import requests
from requests.auth import HTTPBasicAuth

# 1) Load config
with open("config.json") as f:
    config = json.load(f)

CLIENT_ID = config["client_id"]         # your "App ID"
CLIENT_SECRET = config["client_secret"] # your "secret"
USERNAME = config["username"]
PASSWORD = config["password"]
USER_AGENT = config["user_agent"]       # e.g., "myapp/0.1 by u/YOUR_USERNAME"

# 2) Get access token
auth = HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
data = {
    "grant_type": "password",
    "username": USERNAME,
    "password": PASSWORD,
}
headers = {"User-Agent": USER_AGENT}

r = requests.post(
    "https://www.reddit.com/api/v1/access_token",
    auth=auth,
    data=data,
    headers=headers,
)
# Helpful if something goes wrong:
try:
    r.raise_for_status()
except requests.HTTPError:
    print("Token request failed:", r.status_code, r.text)
    raise

token = r.json()["access_token"]

# 3) Use token for API calls
oauth_headers = {
    "Authorization": f"bearer {token}",
    "User-Agent": USER_AGENT,
}

# Example: who am I?
me = requests.get("https://oauth.reddit.com/api/v1/me", headers=oauth_headers)
me.raise_for_status()
print("Authenticated as:", me.json().get("name"))

# Example: fetch top 5 hot posts from r/python
resp = requests.get(
    "https://oauth.reddit.com/r/python/hot",
    headers=oauth_headers,
    params={"limit": 5},
)
resp.raise_for_status()
for i, post in enumerate(resp.json()["data"]["children"], 1):
    d = post["data"]
    print(f"{i}. {d['title']} (score {d['score']}) -> https://reddit.com{d['permalink']}")
