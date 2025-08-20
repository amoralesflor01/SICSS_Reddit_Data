import json
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime as dt
import csv
from pathlib import Path

# ---- config & token ----
with open("config.json") as f:
    config = json.load(f)

CLIENT_ID = config["client_id"]
CLIENT_SECRET = config["client_secret"]
USERNAME = config["username"]
PASSWORD = config["password"]
USER_AGENT = config["user_agent"]

auth = HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET)
data = {
    "grant_type": "password",
    "username": USERNAME,
    "password": PASSWORD,
    "scope": "read identity",
}
headers = {"User-Agent": USER_AGENT}

r = requests.post("https://www.reddit.com/api/v1/access_token",
                  auth=auth, data=data, headers=headers)
r.raise_for_status()
token = r.json()["access_token"]

oauth_headers = {
    "Authorization": f"bearer {token}",
    "User-Agent": USER_AGENT,
}

# ---- your backoff helper (kept) ----
def get_with_backoff(url, headers, params=None, tries=5):
    for t in range(tries):
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 429:
            resp.raise_for_status()
            return resp
        retry_after = int(resp.headers.get("retry-after", "2"))
        time.sleep(retry_after if retry_after > 0 else 2 * (t + 1))
    resp.raise_for_status()

def show_rate(resp):
    used  = resp.headers.get("x-ratelimit-used")
    rem   = resp.headers.get("x-ratelimit-remaining")
    reset = resp.headers.get("x-ratelimit-reset")
    print(f"Rate — Used: {used}, Remaining: {rem}, Reset(min): {reset}")

# ---- date helpers (the key fix) ----
def to_epoch_start(date_str: str) -> int:
    # 00:00:00 UTC at the start of the day
    return int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())

def to_epoch_end(date_str: str) -> int:
    # 23:59:59 UTC at the end of the day
    base = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return int((base + dt.timedelta(days=1, seconds=-1)).timestamp())

# optional: month windows, if you want month-by-month paging
def month_windows_exact(start_date: str, end_date: str):
    """Yield (start, end) as the exact calendar months overlapping the given range."""
    start = dt.datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = dt.datetime.strptime(end_date, "%Y-%m-%d").date()
    cur = start.replace(day=1)
    while cur <= end.replace(day=1):
        nxt = (cur.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        last = nxt - dt.timedelta(days=1)
        # clamp to user range
        s = max(cur, start)
        e = min(last, end)
        yield (s.isoformat(), e.isoformat())
        cur = nxt

def fetch_posts_via_listing(subreddit, start_date, end_date, headers, limit_per_page=100, max_pages=200):
    """
    Crawl /r/{sub}/new (listing) and filter by created_utc between start_date..end_date (inclusive).
    Stops early once items fall below start epoch.
    """
    def to_epoch_start(date_str: str) -> int:
        import datetime as dt
        return int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())
    def to_epoch_end(date_str: str) -> int:
        import datetime as dt
        base = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
        return int((base + dt.timedelta(days=1, seconds=-1)).timestamp())

    start = to_epoch_start(start_date)
    end   = to_epoch_end(end_date)

    url = f"https://oauth.reddit.com/r/{subreddit}/new"
    params = {"limit": min(100, limit_per_page)}
    results = []
    after = None
    pages = 0

    while pages < max_pages:
        if after:
            params["after"] = after
        resp = get_with_backoff(url, headers, params=params)
        show_rate(resp)
        data = resp.json().get("data", {})
        children = data.get("children", [])
        if not children:
            break

        stop = False
        for c in children:
            d = c["data"]
            ts = d.get("created_utc", 0)
            if ts is None:
                continue
            # listing is newest->older; if we've gone earlier than start, we can stop the crawl
            if ts < start:
                stop = True
                continue
            if start <= ts <= end:
                results.append({
                    "id": d.get("id"),
                    "title": d.get("title"),
                    "author": d.get("author"),
                    "created_utc": ts,
                    "score": d.get("score"),
                    "permalink": f"https://reddit.com{d.get('permalink')}",
                    "subreddit": d.get("subreddit"),
                })
        if stop:
            break

        after = data.get("after")
        if not after:
            break
        pages += 1

    return results

def scrape_to_csv_via_listing(subs, global_start, global_end, out_csv):
    rows = []
    for sub in subs:
        print(f"Crawling r/{sub} {global_start}..{global_end} via /new ...")
        posts = fetch_posts_via_listing(sub, global_start, global_end, oauth_headers)
        rows.extend(posts)
        print(f"  -> {len(posts)} posts")
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["id", "title", "author", "created_utc", "score", "permalink", "subreddit"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    print(f"Wrote {len(rows)} rows -> {out_csv}")


# ===== configure your run here =====
SUBREDDITS = ["apartmentliving"]     # use canonical lowercase
SUBREDDITS = ["apartmentliving"]   # no "r/"
GLOBAL_START = "2025-07-19"
GLOBAL_END   = "2025-08-19"
OUT_CSV = "csv_data/reddit_posts.csv"

scrape_to_csv_via_listing(SUBREDDITS, GLOBAL_START, GLOBAL_END, OUT_CSV)

