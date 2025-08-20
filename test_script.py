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

# ---- backoff helper (keep) ----
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

# ---- date helpers ----
def to_epoch_start(date_str: str) -> int:
    return int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())

def to_epoch_end(date_str: str) -> int:
    base = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return int((base + dt.timedelta(days=1, seconds=-1)).timestamp())

def iso_utc_from_epoch(s: int) -> str:
    return dt.datetime.utcfromtimestamp(int(s)).replace(tzinfo=dt.timezone.utc).isoformat()

# ---- LISTING crawl (/new) so we don't rely on search ----
def fetch_posts_via_listing(subreddit, start_date, end_date, headers, limit_per_page=100, max_pages=200):
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
            ts = d.get("created_utc", 0) or 0
            if ts < start:
                stop = True  # we’ve gone older than start range; safe to stop
                continue
            if start <= ts <= end:
                results.append({
                    "id": d.get("id"),
                    "title": d.get("title"),
                    "author": d.get("author"),
                    "created_utc": ts,
                    "created_dt": iso_utc_from_epoch(ts),   # <-- human-readable
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

def dedupe_by_id(rows):
    seen = set()
    out = []
    for r in rows:
        pid = r.get("id")
        if pid and pid not in seen:
            out.append(r)
            seen.add(pid)
    return out

def write_posts_csv(rows, out_csv):
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["id", "title", "author", "created_utc", "created_dt", "score", "permalink", "subreddit"]
    # sort newest -> oldest for convenience
    rows_sorted = sorted(rows, key=lambda x: x.get("created_utc", 0), reverse=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows_sorted)
    print(f"Wrote {len(rows_sorted)} posts -> {out_csv}")

def write_user_counts_csv(rows, out_csv_counts):
    # aggregate per author
    stats = {}
    for r in rows:
        author = r.get("author") or "[deleted]"
        if author not in stats:
            stats[author] = {
                "author": author,
                "post_count": 0,
                "first_post_dt": r.get("created_dt"),
                "last_post_dt": r.get("created_dt"),
                "sum_score": 0,
            }
        stats[author]["post_count"] += 1
        stats[author]["sum_score"] += (r.get("score") or 0)

        # update first/last (chronological)
        cur = r.get("created_dt")
        if cur:
            if stats[author]["first_post_dt"] is None or cur < stats[author]["first_post_dt"]:
                stats[author]["first_post_dt"] = cur
            if stats[author]["last_post_dt"] is None or cur > stats[author]["last_post_dt"]:
                stats[author]["last_post_dt"] = cur

    # convert to rows; compute avg_score
    out_rows = []
    for a, s in stats.items():
        pc = s["post_count"]
        avg = (s["sum_score"] / pc) if pc else 0
        out_rows.append({
            "author": a,
            "post_count": pc,
            "avg_score": round(avg, 2),
            "first_post_dt": s["first_post_dt"],
            "last_post_dt": s["last_post_dt"],
        })

    # sort by post_count desc
    out_rows.sort(key=lambda x: x["post_count"], reverse=True)

    Path(out_csv_counts).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["author", "post_count", "avg_score", "first_post_dt", "last_post_dt"]
    with open(out_csv_counts, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {len(out_rows)} users -> {out_csv_counts}")

def scrape_to_csv_via_listing(subs, global_start, global_end, out_csv_posts, out_csv_user_counts):
    all_rows = []
    for sub in subs:
        print(f"Crawling r/{sub} {global_start}..{global_end} via /new ...")
        rows = fetch_posts_via_listing(sub, global_start, global_end, oauth_headers)
        print(f"  -> {len(rows)} posts")
        all_rows.extend(rows)

    all_rows = dedupe_by_id(all_rows)
    if not all_rows:
        print("No rows to write.")
        return
    write_posts_csv(all_rows, out_csv_posts)
    write_user_counts_csv(all_rows, out_csv_user_counts)

# ===== configure your run here =====
SUBREDDITS = ["apartmentliving", "memes"]     # no 'r/' prefix,  
GLOBAL_START = "2025-07-30"
GLOBAL_END   = "2025-08-10"
OUT_POSTS_CSV = "csv_data/reddit_posts.csv"
OUT_USERS_CSV = "csv_data/user_post_counts.csv"

scrape_to_csv_via_listing(SUBREDDITS, GLOBAL_START, GLOBAL_END, OUT_POSTS_CSV, OUT_USERS_CSV)
