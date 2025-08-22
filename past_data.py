import json
import requests
from requests.auth import HTTPBasicAuth
import time
import datetime as dt
import csv
from pathlib import Path


# ---- Configuration & Authentication ----
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

# ---- Helper Functions ----
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

def to_epoch_start(date_str: str) -> int:
    return int(dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())

def to_epoch_end(date_str: str) -> int:
    base = dt.datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    return int((base + dt.timedelta(days=1, seconds=-1)).timestamp())

def format_post_data(post_data):
    """Standardize post data format regardless of source"""
    d = post_data
    
    # Get post content - handle different post types
    post_content = ""
    if d.get("is_self", False):  # Text post
        post_content = d.get("selftext", "")
    elif d.get("url"):  # Link post
        post_content = f"Link post: {d.get('url')}"
    
    return {
        "post_id": d.get("id"),
        "post_title": d.get("title"),
        "username": d.get("author"),
        "created_utc_unix": d.get("created_utc"),
        "votes": d.get("score"),
        "reddit_url": f"https://reddit.com{d.get('permalink')}",
        "subreddit": d.get("subreddit"),
        "post_content": post_content,
        "is_text": d.get("is_self", False),
        "url_post": d.get("url", ""),
        "comments": d.get("num_comments", 0)
    }

# ---- Method 1: Standard /new listing (works for recent data) ----
def fetch_posts_via_listing(subreddit, start_date, end_date, headers, target_posts=1000):
    """Original method - works well for recent dates or low-activity subreddits"""
    start = to_epoch_start(start_date)
    end = to_epoch_end(end_date)

    url = f"https://oauth.reddit.com/r/{subreddit}/new"
    params = {"limit": 100}
    results = []
    after = None
    pages = 0
    max_pages = 100  # Reasonable limit

    print(f"  Method 1: Trying /new listing for r/{subreddit}")

    while pages < max_pages and len(results) < target_posts:
        if after:
            params["after"] = after
        
        resp = get_with_backoff(url, headers, params=params)
        show_rate(resp)
        data = resp.json().get("data", {})
        children = data.get("children", [])
        
        if not children:
            print(f"    No more posts found after {pages} pages")
            break

        stop_crawling = False
        for c in children:
            d = c["data"]
            ts = d.get("created_utc", 0)
            if ts is None:
                continue
            
            # If we've gone past our start date, we're done
            if ts < start:
                print(f"    Reached end of date range")
                stop_crawling = True
                break
            
            # Only collect posts within our date range    
            if start <= ts <= end:
                results.append(format_post_data(d))
                
                if len(results) >= target_posts:
                    print(f"    Reached target of {target_posts} posts!")
                    stop_crawling = True
                    break
                    
        if stop_crawling:
            break

        after = data.get("after")
        if not after:
            break
        pages += 1

    print(f"    Method 1 result: {len(results)} posts")
    return results

# ---- Method 2: Search API (better for historical data) ----
def fetch_posts_via_search(subreddit, start_date, end_date, headers, target_posts=1000):
    """Search-based method - better for historical dates"""
    start = to_epoch_start(start_date)
    end = to_epoch_end(end_date)
    
    print(f"  Method 2: Trying search API for r/{subreddit}")
    
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    results = []
    
    # Try different search strategies
    search_strategies = [
        {"q": "*", "sort": "new"},  # Search all posts, sort by new
        {"q": "title:*", "sort": "new"},  # Search by title
        {"q": "selftext:*", "sort": "new"}  # Search by content
    ]
    
    for strategy in search_strategies:
        if len(results) >= target_posts:
            break
            
        params = {
            **strategy,
            "restrict_sr": "true",
            "limit": 100,
            "t": "all"
        }
        
        pages = 0
        after = None
        max_search_pages = 50
        
        while pages < max_search_pages and len(results) < target_posts:
            if after:
                params["after"] = after
                
            try:
                resp = get_with_backoff(url, headers, params=params)
                show_rate(resp)
                data = resp.json().get("data", {})
                children = data.get("children", [])
                
                if not children:
                    break
                    
                page_results = 0
                for c in children:
                    d = c["data"]
                    ts = d.get("created_utc", 0)
                    if ts is None:
                        continue
                        
                    # Check if post is in our date range
                    if start <= ts <= end:
                        formatted_post = format_post_data(d)
                        # Avoid duplicates
                        if not any(p["post_id"] == formatted_post["post_id"] for p in results):
                            results.append(formatted_post)
                            page_results += 1
                            
                    if len(results) >= target_posts:
                        break
                
                if page_results == 0:  # No new results in range
                    break
                    
                after = data.get("after")
                if not after:
                    break
                pages += 1
                
            except Exception as e:
                print(f"    Search error: {e}")
                break
    
    print(f"    Method 2 result: {len(results)} posts")
    return results

# ---- Method 3: Pushshift fallback (for comprehensive historical data) ----
def fetch_posts_via_pushshift(subreddit, start_date, end_date, target_posts=1000):
    """Pushshift API fallback for comprehensive historical data"""
    print(f"  Method 3: Trying Pushshift for r/{subreddit}")
    
    start = to_epoch_start(start_date)
    end = to_epoch_end(end_date)
    
    # Note: Pushshift may be unreliable - this is a template
    url = "https://api.pushshift.io/reddit/search/submission/"
    params = {
        "subreddit": subreddit,
        "after": start,
        "before": end,
        "size": min(500, target_posts),
        "sort": "created_utc",
        "sort_type": "created_utc"
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 200:
            data = resp.json().get("data", [])
            results = []
            
            for post in data[:target_posts]:
                # Convert Pushshift format to Reddit format
                formatted_post = {
                    "post_id": post.get("id"),
                    "post_title": post.get("title"),
                    "username": post.get("author"),
                    "created_utc_unix": post.get("created_utc"),
                    "votes": post.get("score", 0),
                    "reddit_url": f"https://reddit.com{post.get('permalink', '')}",
                    "subreddit": post.get("subreddit"),
                    "post_content": post.get("selftext", ""),
                    "is_text": post.get("is_self", False),
                    "url_post": post.get("url", ""),
                    "comments": post.get("num_comments", 0)
                }
                results.append(formatted_post)
            
            print(f"    Method 3 result: {len(results)} posts")
            return results
    except Exception as e:
        print(f"    Pushshift error: {e}")
    
    return []

# ---- Temporal Distribution Helpers ----
def create_time_windows(start_date, end_date, num_windows):
    """Create evenly spaced time windows across the date range"""
    start = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end = dt.datetime.strptime(end_date, "%Y-%m-%d")
    
    total_days = (end - start).days
    days_per_window = max(1, total_days // num_windows)
    
    windows = []
    current_start = start
    
    for i in range(num_windows):
        window_end = current_start + dt.timedelta(days=days_per_window)
        if i == num_windows - 1:  # Last window gets any remaining days
            window_end = end
        
        windows.append({
            "start": current_start.strftime("%Y-%m-%d"),
            "end": window_end.strftime("%Y-%m-%d"),
            "window_num": i + 1
        })
        
        current_start = window_end + dt.timedelta(days=1)
        if current_start > end:
            break
    
    return windows

def collect_posts_with_temporal_distribution(subreddit, start_date, end_date, headers, target_posts=1000):
    """
    Collect posts with even temporal distribution across the date range.
    Instead of clustering posts from one time period, spread them evenly.
    """
    print(f"\n--- Collecting posts for r/{subreddit} with temporal distribution ---")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Target: {target_posts} posts with even time spread")
    
    # Create time windows (aim for ~8-12 windows for good distribution)
    total_days = (dt.datetime.strptime(end_date, "%Y-%m-%d") - dt.datetime.strptime(start_date, "%Y-%m-%d")).days
    num_windows = min(12, max(6, total_days // 15))  # 6-12 windows, ~15 days each
    
    time_windows = create_time_windows(start_date, end_date, num_windows)
    posts_per_window = target_posts // len(time_windows)
    remainder_posts = target_posts % len(time_windows)
    
    print(f"  Strategy: {len(time_windows)} time windows, ~{posts_per_window} posts each")
    
    all_posts = []
    methods_used = set()
    temporal_distribution = []
    
    for i, window in enumerate(time_windows):
        window_target = posts_per_window
        if i < remainder_posts:  # Distribute remainder posts to first few windows
            window_target += 1
        
        print(f"\n  Window {window['window_num']}: {window['start']} to {window['end']} (target: {window_target})")
        
        window_posts = []
        
        # Try listing first for this window
        if window_target > 0:
            listing_posts = fetch_posts_via_listing(subreddit, window['start'], window['end'], headers, window_target)
            if listing_posts:
                window_posts.extend(listing_posts[:window_target])
                methods_used.add("listing")
                print(f"    Listing method: {len(listing_posts)} posts")
        
        # If we need more posts for this window, try search
        if len(window_posts) < window_target:
            remaining = window_target - len(window_posts)
            search_posts = fetch_posts_via_search(subreddit, window['start'], window['end'], headers, remaining)
            
            # Remove duplicates and add
            existing_ids = {p["post_id"] for p in window_posts}
            unique_search = [p for p in search_posts if p["post_id"] not in existing_ids]
            
            if unique_search:
                window_posts.extend(unique_search[:remaining])
                methods_used.add("search")
                print(f"    Search method: {len(unique_search)} additional posts")
        
        # If still need more, try Pushshift for this window
        if len(window_posts) < window_target * 0.5:  # If we have less than half
            remaining = window_target - len(window_posts)
            pushshift_posts = fetch_posts_via_pushshift(subreddit, window['start'], window['end'], remaining)
            
            existing_ids = {p["post_id"] for p in window_posts}
            unique_pushshift = [p for p in pushshift_posts if p["post_id"] not in existing_ids]
            
            if unique_pushshift:
                window_posts.extend(unique_pushshift[:remaining])
                methods_used.add("pushshift")
                print(f"    Pushshift method: {len(unique_pushshift)} additional posts")
        
        # Record temporal distribution
        temporal_distribution.append({
            "window": f"{window['start']} to {window['end']}",
            "target_posts": window_target,
            "actual_posts": len(window_posts),
            "coverage_percent": round((len(window_posts) / window_target) * 100, 1) if window_target > 0 else 0
        })
        
        all_posts.extend(window_posts)
        print(f"    Window result: {len(window_posts)}/{window_target} posts")
        
        # Small delay between windows
        time.sleep(0.5)
    
    # Sort all posts by timestamp for final output
    all_posts.sort(key=lambda x: x.get("created_utc_unix", 0), reverse=True)
    
    print(f"\n  TEMPORAL DISTRIBUTION SUMMARY:")
    for dist in temporal_distribution:
        print(f"    {dist['window']}: {dist['actual_posts']}/{dist['target_posts']} posts ({dist['coverage_percent']}%)")
    
    print(f"\n  FINAL: {len(all_posts)} posts collected using methods: {', '.join(methods_used)}")
    print(f"  Temporal spread: {len([d for d in temporal_distribution if d['actual_posts'] > 0])} time periods with data")
    
    return all_posts, list(methods_used), temporal_distribution

# ---- Hybrid Collection Strategy (Updated) ----
def collect_posts_hybrid(subreddit, start_date, end_date, headers, target_posts=1000):
    """
    Choose between temporal distribution and standard collection based on date range
    """
    total_days = (dt.datetime.strptime(end_date, "%Y-%m-%d") - dt.datetime.strptime(start_date, "%Y-%m-%d")).days
    
    # Use temporal distribution for longer date ranges (more than 30 days)
    if total_days > 30:
        print(f"  Using temporal distribution strategy (date range: {total_days} days)")
        posts, methods, temporal_dist = collect_posts_with_temporal_distribution(subreddit, start_date, end_date, headers, target_posts)
        return posts, methods
    else:
        print(f"  Using standard collection strategy (date range: {total_days} days)")
        # Original hybrid logic for shorter ranges
        all_posts = []
        methods_used = []
        
        # Method 1: Standard listing
        posts_listing = fetch_posts_via_listing(subreddit, start_date, end_date, headers, target_posts)
        if posts_listing:
            all_posts.extend(posts_listing)
            methods_used.append("listing")
        
        # Method 2: Search API (if we need more posts)
        if len(all_posts) < target_posts * 0.8:
            remaining_needed = target_posts - len(all_posts)
            posts_search = fetch_posts_via_search(subreddit, start_date, end_date, headers, remaining_needed)
            
            existing_ids = {p["post_id"] for p in all_posts}
            unique_search_posts = [p for p in posts_search if p["post_id"] not in existing_ids]
            
            if unique_search_posts:
                all_posts.extend(unique_search_posts)
                methods_used.append("search")
        
        # Method 3: Pushshift fallback (if still insufficient)
        if len(all_posts) < target_posts * 0.5:
            remaining_needed = target_posts - len(all_posts)
            posts_pushshift = fetch_posts_via_pushshift(subreddit, start_date, end_date, remaining_needed)
            
            existing_ids = {p["post_id"] for p in all_posts}
            unique_pushshift_posts = [p for p in posts_pushshift if p["post_id"] not in existing_ids]
            
            if unique_pushshift_posts:
                all_posts.extend(unique_pushshift_posts)
                methods_used.append("pushshift")
        
        all_posts.sort(key=lambda x: x.get("created_utc_unix", 0), reverse=True)
        final_posts = all_posts[:target_posts]
        
        print(f"  FINAL: {len(final_posts)} posts collected using methods: {', '.join(methods_used)}")
        return final_posts, methods_used

# ---- Main Collection Function ----
def scrape_to_csv_research_grade(subs, global_start, global_end, output_dir="csv_data", posts_per_sub=1000):
    """Research-grade scraper with multiple collection methods"""
    
    total_posts = 0
    created_files = []
    collection_report = []
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    fieldnames = [
        "post_id", "post_title", "username", "created_utc_unix", "votes", "reddit_url", "subreddit",
        "post_content", "is_text", "url_post", "comments"
    ]
    
    print(f"\n=== RESEARCH-GRADE REDDIT SCRAPER ===")
    print(f"Target period: {global_start} to {global_end}")
    print(f"Subreddits: {subs}")
    print(f"Posts per subreddit: {posts_per_sub}")
    print("=" * 60)
    
    for sub in subs:
        posts, methods_used = collect_posts_hybrid(sub, global_start, global_end, oauth_headers, posts_per_sub)
        
        collection_report.append({
            "subreddit": sub,
            "posts_collected": len(posts),
            "methods_used": methods_used,
            "coverage_percent": round((len(posts) / posts_per_sub) * 100, 1)
        })
        
        if posts:
            filename = f"{sub}_data.csv"
            filepath = Path(output_dir) / filename
            
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(posts)
            
            created_files.append(str(filepath))
            total_posts += len(posts)
            
            # Stats
            text_posts = sum(1 for row in posts if row.get('is_text'))
            link_posts = len(posts) - text_posts
            print(f"  ✓ Saved to: {filepath}")
            print(f"    Text posts: {text_posts}, Link posts: {link_posts}")
        else:
            print(f"  ✗ No posts found for r/{sub}")
        
        time.sleep(2)  # Be nice to APIs
    
    # Generate research report
    print(f"\n=== COLLECTION REPORT ===")
    print(f"Total subreddits: {len(subs)}")
    print(f"Total posts collected: {total_posts}")
    print(f"Files created: {len(created_files)}")
    
    print(f"\nDETAILED BREAKDOWN:")
    for report in collection_report:
        methods_str = ", ".join(report["methods_used"]) if report["methods_used"] else "none"
        print(f"  r/{report['subreddit']}: {report['posts_collected']} posts ({report['coverage_percent']}% of target) via {methods_str}")
    
    # Save collection metadata for research documentation
    metadata_file = Path(output_dir) / "collection_metadata.json"
    metadata = {
        "collection_date": dt.datetime.now().isoformat(),
        "date_range": {"start": global_start, "end": global_end},
        "target_posts_per_sub": posts_per_sub,
        "total_posts_collected": total_posts,
        "subreddits": collection_report,
        "files_created": created_files
    }
    
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n✓ Research metadata saved to: {metadata_file}")
    print(f"\nFILES READY FOR ANALYSIS:")
    for file in created_files:
        print(f"  - {file}")
    
    return created_files, collection_report

# ===== RESEARCH CONFIGURATION =====
# Modify these for your specific research needs

RESEARCH_NAME = "Political Discussion Analysis 2025"
SUBREDDITS = [
    # "politics", 
    "politicaldiscussion", 
    # "immigration"
]

# Your specific date ranges
GLOBAL_START = "2025-01-01" 
GLOBAL_END   = "2025-07-01"  
OUTPUT_DIR = "past_data"
POSTS_PER_SUBREDDIT = 1000

# Execute collection
if __name__ == "__main__":
    created_files, report = scrape_to_csv_research_grade(
        SUBREDDITS, 
        GLOBAL_START, 
        GLOBAL_END, 
        OUTPUT_DIR, 
        POSTS_PER_SUBREDDIT
    )