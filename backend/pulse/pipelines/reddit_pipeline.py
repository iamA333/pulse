"""reddit_pipeline.py

Simple pipeline to fetch posts from a list of subreddits using Reddit's public JSON
endpoints and save each subreddit's posts to a JSON file under data/processed/.

Usage:
    python -m backend.pulse.pipelines.reddit_pipeline

Environment variables:
    REDDIT_SUBREDDITS - comma-separated subreddit list (default: python,learnpython,datascience)
    REDDIT_LIMIT      - number of posts to fetch per subreddit (default: 50)
    OUTPUT_DIR        - base output dir (default: repo-root/data/processed)

Note: This uses reddit public JSON endpoints and sets a custom User-Agent. For large-scale
or authenticated access, swap to OAuth and use official APIs.
"""

import json
import os
import time
from typing import List, Dict, Any

import requests


USER_AGENT = "pulse-backend/0.1 (by iamA333)"


def fetch_subreddit(subreddit: str, limit: int = 50, time_filter: str = "day") -> List[Dict[str, Any]]:
    """Fetch posts from the subreddit 'hot' listing (public JSON endpoint).

    Returns a list of simplified post dicts.
    """
    url = f"https://www.reddit.com/r/{subreddit}/hot.json"
    params = {"limit": limit, "t": time_filter}
    headers = {"User-Agent": USER_AGENT}

    resp = requests.get(url, params=params, headers=headers, timeout=10)
    resp.raise_for_status()
    payload = resp.json()

    posts = []
    for child in payload.get("data", {}).get("children", []):
        d = child.get("data", {})
        posts.append(
            {
                "id": d.get("id"),
                "title": d.get("title"),
                "author": d.get("author"),
                "created_utc": d.get("created_utc"),
                "score": d.get("score"),
                "num_comments": d.get("num_comments"),
                "url": d.get("url"),
                "selftext": d.get("selftext"),
                "subreddit": d.get("subreddit"),
            }
        )

    return posts


def save_json(data: Any, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def run(subreddits: List[str], limit: int, out_dir: str) -> None:
    results = {}
    for sub in subreddits:
        try:
            print(f"Fetching r/{sub} (limit={limit})")
            posts = fetch_subreddit(sub, limit=limit)
            results[sub] = {"fetched": len(posts), "posts": posts}

            out_path = os.path.join(out_dir, f"reddit_{sub}.json")
            save_json(results[sub], out_path)
            print(f"Saved {len(posts)} posts to {out_path}")

            # be kind to reddit
            time.sleep(1)
        except Exception as exc:
            print(f"Error fetching r/{sub}: {exc}")


def main():
    env_subs = os.getenv("REDDIT_SUBREDDITS", "python,learnpython,datascience")
    subreddits = [s.strip() for s in env_subs.split(",") if s.strip()]
    limit = int(os.getenv("REDDIT_LIMIT", "50"))

    # default output dir: repo root /data/processed
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "processed"))
    out_dir = os.getenv("OUTPUT_DIR", base_dir)

    run(subreddits, limit, out_dir)


if __name__ == "__main__":
    main()
