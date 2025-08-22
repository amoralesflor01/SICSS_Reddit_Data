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
        "comments": d.get("num_comments", 0),
        "created_date": dt.datetime.fromtimestamp(d.get("created_utc", 0), dt.timezone.utc).strftime("%Y-%m-%d") if d.get("created_utc") else ""
    }


def fetch_posts_via_enhanced_search(subreddit, start_date, end_date, headers, target_posts=1000):
    """Enhanced search with multiple strategies for better historical coverage"""
    start = to_epoch_start(start_date)
    end = to_epoch_end(end_date)
    
    print(f"  Enhanced Search for r/{subreddit} ({start_date} to {end_date})")
    
    url = f"https://oauth.reddit.com/r/{subreddit}/search"
    results = []
    
    # More comprehensive search strategies
    search_strategies = [
        # Broad searches
        {"q": "*", "sort": "new", "t": "all"},
        {"q": "the", "sort": "new", "t": "all"},  # Very common word
        {"q": "a", "sort": "new", "t": "all"},    # Even more common
        
        # Time-specific searches
        {"q": "2025", "sort": "new", "t": "all"} if "2025" in start_date else {"q": "2024", "sort": "new", "t": "all"},
        
        # Content-based searches
        {"q": "title:*", "sort": "new", "t": "all"},
        {"q": "selftext:*", "sort": "new", "t": "all"},
        
        # Different sorting methods
        {"q": "*", "sort": "relevance", "t": "all"},
        {"q": "*", "sort": "hot", "t": "all"},
        {"q": "*", "sort": "top", "t": "all"},
        
        # Empty search (sometimes works)
        {"q": "", "sort": "new", "t": "all"},
    ]
    
    for i, strategy in enumerate(search_strategies):
        if len(results) >= target_posts:
            break
            
        print(f"    Strategy {i+1}: {strategy}")
        
        params = {
            **strategy,
            "restrict_sr": "true",
            "limit": 100,
        }
        
        pages = 0
        after = None
        max_search_pages = 20  # Increased pages per strategy
        strategy_results = 0
        
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
                            strategy_results += 1
                            
                    if len(results) >= target_posts:
                        break
                
                after = data.get("after")
                if not after:
                    break
                pages += 1
                
                # Small delay between pages
                time.sleep(0.3)
                
            except Exception as e:
                print(f"    Strategy {i+1} error: {e}")
                break
        
        print(f"    Strategy {i+1} found: {strategy_results} new posts")
        
        # Delay between strategies
        time.sleep(1)
    
    print(f"    Enhanced search result: {len(results)} posts")
    return results


def fetch_posts_via_improved_listing(subreddit, start_date, end_date, headers, target_posts=1000):
    """Improved listing method with different sorting options"""
    start = to_epoch_start(start_date)
    end = to_epoch_end(end_date)
    
    print(f"  Improved Listing for r/{subreddit}")
    
    # Try different endpoints
    endpoints = [
        f"https://oauth.reddit.com/r/{subreddit}/new",
        f"https://oauth.reddit.com/r/{subreddit}/hot", 
        f"https://oauth.reddit.com/r/{subreddit}/top",
        f"https://oauth.reddit.com/r/{subreddit}/rising"
    ]
    
    all_results = []
    
    for endpoint in endpoints:
        if len(all_results) >= target_posts:
            break
            
        print(f"    Trying endpoint: {endpoint.split('/')[-1]}")
        
        params = {"limit": 100, "t": "all"} if "top" in endpoint else {"limit": 100}
        after = None
        pages = 0
        max_pages = 50  # More pages for historical data
        
        while pages < max_pages and len(all_results) < target_posts:
            if after:
                params["after"] = after
            
            try:
                resp = get_with_backoff(endpoint, headers, params=params)
                show_rate(resp)
                data = resp.json().get("data", {})
                children = data.get("children", [])
                
                if not children:
                    break

                found_in_range = False
                for c in children:
                    d = c["data"]
                    ts = d.get("created_utc", 0)
                    if ts is None:
                        continue
                    
                    # For new posts, stop if we've gone too far back
                    if "new" in endpoint and ts < start:
                        print(f"    Reached date limit at page {pages}")
                        return all_results
                    
                    # Collect posts within our date range    
                    if start <= ts <= end:
                        formatted_post = format_post_data(d)
                        # Avoid duplicates across endpoints
                        if not any(p["post_id"] == formatted_post["post_id"] for p in all_results):
                            all_results.append(formatted_post)
                            found_in_range = True
                            
                            if len(all_results) >= target_posts:
                                return all_results
                
                # If no posts found in range for several pages, try next endpoint
                if not found_in_range and pages > 10:
                    break

                after = data.get("after")
                if not after:
                    break
                pages += 1
                
                time.sleep(0.2)
                
            except Exception as e:
                print(f"    Endpoint error: {e}")
                break
        
        print(f"    Endpoint {endpoint.split('/')[-1]}: {len(all_results)} total posts so far")
    
    print(f"    Improved listing result: {len(all_results)} posts")
    return all_results


def collect_posts_weekly_windows(subreddit, start_date, end_date, headers, target_posts=1000):
    """Collect posts using weekly time windows for better temporal distribution"""
    print(f"\n--- Weekly collection strategy for r/{subreddit} ---")
    print(f"Date range: {start_date} to {end_date}")
    
    # Create weekly windows
    start_dt = dt.datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = dt.datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start_dt
    windows = []
    
    while current < end_dt:
        window_end = min(current + dt.timedelta(days=7), end_dt)
        windows.append({
            "start": current.strftime("%Y-%m-%d"),
            "end": window_end.strftime("%Y-%m-%d")
        })
        current = window_end + dt.timedelta(days=1)
    
    posts_per_window = max(10, target_posts // len(windows))
    print(f"  Created {len(windows)} weekly windows, ~{posts_per_window} posts each")
    
    all_posts = []
    
    for i, window in enumerate(windows):
        print(f"\n  Week {i+1}: {window['start']} to {window['end']}")
        
        # Try enhanced search first for each week
        week_posts = fetch_posts_via_enhanced_search(
            subreddit, window['start'], window['end'], headers, posts_per_window
        )
        
        # If not enough, try improved listing
        if len(week_posts) < posts_per_window * 0.3:
            additional = fetch_posts_via_improved_listing(
                subreddit, window['start'], window['end'], headers, posts_per_window
            )
            
            # Merge without duplicates
            existing_ids = {p["post_id"] for p in week_posts}
            unique_additional = [p for p in additional if p["post_id"] not in existing_ids]
            week_posts.extend(unique_additional[:posts_per_window - len(week_posts)])
        
        all_posts.extend(week_posts)
        print(f"    Week {i+1} collected: {len(week_posts)} posts")
        
        # Longer delay between weeks to be respectful
        time.sleep(3)
    
    # Sort by timestamp and limit to target
    all_posts.sort(key=lambda x: x.get("created_utc_unix", 0), reverse=True)
    final_posts = all_posts[:target_posts]
    
    print(f"\n  Weekly strategy result: {len(final_posts)} posts across {len(windows)} weeks")
    return final_posts


def scrape_to_csv_comprehensive(subs, global_start, global_end, output_dir="csv_data", posts_per_sub=1000):
    """Comprehensive scraper optimized for historical data collection"""
    
    total_posts = 0
    created_files = []
    collection_report = []
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    fieldnames = [
        "post_id", "post_title", "username", "created_utc_unix", "votes", "reddit_url", "subreddit",
        "post_content", "is_text", "url_post", "comments", "created_date"
    ]
    
    print("\n=== COMPREHENSIVE REDDIT SCRAPER ===")
    print(f"Target period: {global_start} to {global_end}")
    print(f"Subreddits: {subs}")
    print(f"Posts per subreddit: {posts_per_sub}")
    print("=" * 60)
    
    for sub in subs:
        print(f"\n--- Processing r/{sub} ---")
        
        # Use weekly collection strategy for better coverage
        posts = collect_posts_weekly_windows(sub, global_start, global_end, oauth_headers, posts_per_sub)
        
        # If still not enough posts, try the enhanced methods
        if len(posts) < posts_per_sub * 0.5:
            print(f"  Insufficient data ({len(posts)} posts). Trying enhanced methods...")
            
            # Enhanced search
            additional_search = fetch_posts_via_enhanced_search(
                sub, global_start, global_end, oauth_headers, posts_per_sub - len(posts)
            )
            
            # Merge without duplicates
            existing_ids = {p["post_id"] for p in posts}
            unique_search = [p for p in additional_search if p["post_id"] not in existing_ids]
            posts.extend(unique_search)
            
            # Enhanced listing if still needed
            if len(posts) < posts_per_sub * 0.3:
                additional_listing = fetch_posts_via_improved_listing(
                    sub, global_start, global_end, oauth_headers, posts_per_sub - len(posts)
                )
                
                existing_ids = {p["post_id"] for p in posts}
                unique_listing = [p for p in additional_listing if p["post_id"] not in existing_ids]
                posts.extend(unique_listing)
        
        collection_report.append({
            "subreddit": sub,
            "posts_collected": len(posts),
            "coverage_percent": round((len(posts) / posts_per_sub) * 100, 1),
            "date_range_covered": f"{global_start} to {global_end}"
        })
        
        if posts:
            filename = f"{sub}_data_{global_start}_to_{global_end}.csv"
            filepath = Path(output_dir) / filename
            
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames)
                w.writeheader()
                w.writerows(posts)
            
            created_files.append(str(filepath))
            total_posts += len(posts)
            
            # Date distribution analysis
            dates = [p["created_date"] for p in posts if p["created_date"]]
            unique_dates = len(set(dates))
            
            print(f"  ✓ Saved to: {filepath}")
            print(f"    Posts: {len(posts)}, Unique dates: {unique_dates}")
            
            # Show temporal distribution
            if dates:
                earliest = min(dates)
                latest = max(dates)
                print(f"    Date range in data: {earliest} to {latest}")
        else:
            print(f"  ✗ No posts found for r/{sub}")
        
        # Longer delay between subreddits
        time.sleep(5)
    
    # Generate comprehensive report
    print("\n=== COLLECTION REPORT ===")
    print(f"Total subreddits processed: {len(subs)}")
    print(f"Total posts collected: {total_posts}")
    print(f"Average posts per subreddit: {total_posts // len(subs) if subs else 0}")
    
    print("\nDETAILED BREAKDOWN:")
    for report in collection_report:
        print(f"  r/{report['subreddit']}: {report['posts_collected']} posts ({report['coverage_percent']}% of target)")
    
    # Save comprehensive metadata
    metadata_file = Path(output_dir) / f"collection_metadata_{global_start}_to_{global_end}.json"
    metadata = {
        "collection_date": dt.datetime.now().isoformat(),
        "date_range": {"start": global_start, "end": global_end},
        "target_posts_per_sub": posts_per_sub,
        "total_posts_collected": total_posts,
        "subreddits": collection_report,
        "files_created": created_files,
        "collection_strategy": "weekly_windows_with_enhanced_fallbacks"
    }
    
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\n✓ Metadata saved to: {metadata_file}")
    print("\nFILES READY FOR ANALYSIS:")
    for file in created_files:
        print(f"  - {file}")
    
    return created_files, collection_report

# ===== RESEARCH CONFIGURATION =====
RESEARCH_NAME = "Political Discussions Jan-July 2025"
SUBREDDITS = [
    "politics", 
    "politicaldiscussion", 
    "immigration"
]

# Date ranges ("YYYY-MM-DD")
GLOBAL_START = "2025-01-01" 
GLOBAL_END   = "2025-07-01"  
OUTPUT_DIR = "csv_data"
POSTS_PER_SUBREDDIT = 2000  # Increased target since we're covering 7 months


if __name__ == "__main__":
    created_files, report = scrape_to_csv_comprehensive(
        SUBREDDITS, 
        GLOBAL_START, 
        GLOBAL_END, 
        OUTPUT_DIR, 
        POSTS_PER_SUBREDDIT
    )
