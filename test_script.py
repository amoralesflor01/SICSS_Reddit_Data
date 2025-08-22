import json
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime as dt
import csv
from pathlib import Path


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

def fetch_posts_via_listing(subreddit, start_date, end_date, headers, target_posts=1000, limit_per_page=100, max_pages=500):
    """
    Crawl /r/{sub}/new (listing) and filter by created_utc between start_date..end_date (inclusive).
    Stops when EITHER:
    1. We reach target_posts (e.g., 1000), OR  
    2. We go past the end_date (earlier than start epoch), OR
    3. We run out of posts
    Now includes post content (selftext).
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

    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Target: {target_posts} posts for r/{subreddit} (will stop at target OR end of date range)")

    while pages < max_pages and len(results) < target_posts:
        if after:
            params["after"] = after
        resp = get_with_backoff(url, headers, params=params)
        show_rate(resp)
        data = resp.json().get("data", {})
        children = data.get("children", [])
        if not children:
            print(f"  No more posts found after {pages} pages")
            break

        stop_crawling = False
        for c in children:
            d = c["data"]
            ts = d.get("created_utc", 0)
            if ts is None:
                continue
            
            # Since /new is newest->oldest, if we've gone past our start date, we're done
            if ts < start:
                print(f"  Reached end of date range (post from {dt.datetime.fromtimestamp(ts, dt.timezone.utc).date()})")
                stop_crawling = True
                break
            
            # Only collect posts within our date range    
            if start <= ts <= end:
                # Get post content - handle different post types
                post_content = ""
                if d.get("is_self", False):  # Text post
                    post_content = d.get("selftext", "")
                elif d.get("url"):  # Link post
                    post_content = f"Link post: {d.get('url')}"
                
                results.append({
                    "id": d.get("id"),
                    "title": d.get("title"),
                    "author": d.get("author"),
                    "created_utc": ts,
                    "score": d.get("score"),
                    "permalink": f"https://reddit.com{d.get('permalink')}",
                    "subreddit": d.get("subreddit"),
                    "post_content": post_content,  # NEW: actual post content
                    "is_self": d.get("is_self", False),  # NEW: is it a text post?
                    "url": d.get("url", ""),  # NEW: external URL if link post
                    "num_comments": d.get("num_comments", 0),  # NEW: comment count
                })
                
                # Stop if we've reached our target posts
                if len(results) >= target_posts:
                    print(f"  Reached target of {target_posts} posts!")
                    stop_crawling = True
                    break
                    
        if stop_crawling:
            break

        after = data.get("after")
        if not after:
            print(f"  Reached end of available posts after {pages} pages")
            break
        pages += 1
        
        # Progress update every 5 pages (more frequent for better tracking)
        if pages % 5 == 0:
            print(f"  Progress: {len(results)} posts collected after {pages} pages")

    # Final summary for this subreddit
    reason = ""
    if len(results) >= target_posts:
        reason = f"reached target of {target_posts} posts"
    elif pages >= max_pages:
        reason = "hit max pages limit"
    else:
        reason = "exhausted available posts in date range"
    
    print(f"  Finished: {len(results)} posts collected ({reason})")
    return results

def scrape_to_csv_via_listing(subs, global_start, global_end, out_csv, posts_per_sub=1000):
    """
    Enhanced version that collects more data per subreddit and includes post content.
    """
    all_rows = []
    for sub in subs:
        print(f"Crawling r/{sub} {global_start}..{global_end} via /new ...")
        posts = fetch_posts_via_listing(sub, global_start, global_end, oauth_headers, target_posts=posts_per_sub)
        all_rows.extend(posts)
        print(f"  -> {len(posts)} posts collected for r/{sub}")
        
        # Small delay between subreddits to be nice to Reddit's servers
        time.sleep(1)
    
    # Create output directory if it doesn't exist
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    
    # Updated fieldnames to include new columns
    fieldnames = [
        "id", "title", "author", "created_utc", "score", "permalink", "subreddit",
        "post_content", "is_self", "url", "num_comments"
    ]
    
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)
    
    print("\n=== COMPLETE ===")
    print(f"Total posts collected: {len(all_rows)}")
    print(f"Output saved to: {out_csv}")
    
    # Summary stats
    text_posts = sum(1 for row in all_rows if row.get('is_self'))
    link_posts = len(all_rows) - text_posts
    print(f"Text posts: {text_posts}")
    print(f"Link posts: {link_posts}")
    
    return all_rows

# ===== CONFIGURE YOUR RUN HERE =====
SUBREDDITS = ["politics"]     # use canonical lowercase
GLOBAL_START = "2025-07-19"  # START of your date range
GLOBAL_END   = "2025-08-19"  # END of your date range  
OUT_CSV = "csv_data/reddit_posts.csv"
POSTS_PER_SUBREDDIT = 1000  # Will stop at 1000 posts OR end of date range, whichever comes first

print("=== Reddit Scraper Configuration ===")
print(f"Subreddits: {SUBREDDITS}")
print(f"Date range: {GLOBAL_START} to {GLOBAL_END}")
print(f"Target posts per subreddit: {POSTS_PER_SUBREDDIT}")
print(f"Output file: {OUT_CSV}")
print(f"Strategy: Collect up to {POSTS_PER_SUBREDDIT} posts within date range, stop at whichever limit hits first")
print("=" * 50)

scrape_to_csv_via_listing(SUBREDDITS, GLOBAL_START, GLOBAL_END, OUT_CSV, POSTS_PER_SUBREDDIT)