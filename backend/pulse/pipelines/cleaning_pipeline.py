"""cleaning_pipeline.py

Load reddit JSON files from data/processed, deduplicate posts, normalize fields,
and write cleaned JSON files next to the originals with prefix `cleaned_`.

Behavior:
 - Input: by default processes all files matching data/processed/reddit_*.json in repo root
 - Deduplicate by `id` if present; fall back to `url`.
 - Normalize:
     - strip and collapse whitespace in `title` and `selftext`
     - create `created_iso` from `created_utc` when numeric
     - ensure keys exist with None defaults
 - Output: save cleaned JSON with same structure: {"fetched": N, "posts": [...]}

Usage:
    python -m backend.pulse.pipelines.cleaning_pipeline

Or process a single file:
    python d:/project/pulse/backend/pulse/pipelines/cleaning_pipeline.py d:/project/pulse/data/processed/reddit_MachineLearning.json
"""

from __future__ import annotations

import glob
import json
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def collapse_whitespace(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    # normalize to single spaces and strip
    return re.sub(r"\s+", " ", s).strip()


def created_utc_to_iso(val: Any) -> Optional[str]:
    try:
        if val is None:
            return None
        # sometimes it's float or int
        ts = float(val)
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


def normalize_post(d: Dict[str, Any]) -> Dict[str, Any]:
    # Ensure presence of keys and normalize
    post_id = d.get("id") or d.get("post_id") or None
    title = collapse_whitespace(d.get("title") or d.get("selftext") or "")
    selftext = collapse_whitespace(d.get("selftext") or "")
    author = d.get("author")
    created_iso = created_utc_to_iso(d.get("created_utc") or d.get("created"))
    score = d.get("score")
    num_comments = d.get("num_comments")
    url = d.get("url")
    subreddit = d.get("subreddit")

    return {
        "id": post_id,
        "title": title,
        "selftext": selftext,
        "author": author,
        "created_iso": created_iso,
        "score": score,
        "num_comments": num_comments,
        "url": url,
        "subreddit": subreddit,
    }


def dedupe_posts(posts: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen_ids = set()
    seen_urls = set()
    out = []
    for p in posts:
        pid = p.get("id")
        url = p.get("url")
        key = None
        if pid:
            key = ("id", pid)
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
        elif url:
            key = ("url", url)
            if url in seen_urls:
                continue
            seen_urls.add(url)
        else:
            # if neither id nor url, include but it's rare
            pass

        out.append(p)
    return out


def process_file(path: str, out_dir: Optional[str] = None) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    posts = payload.get("posts") if isinstance(payload, dict) and "posts" in payload else payload
    if posts is None:
        raise ValueError(f"No posts found in {path}")

    original_count = len(posts)

    normalized = [normalize_post(p) for p in posts]
    deduped = dedupe_posts(normalized)

    result = {"original_fetched": original_count, "cleaned": len(deduped), "posts": deduped}

    # prepare output path
    base = os.path.basename(path)
    out_dir = out_dir or os.path.dirname(path)
    out_path = os.path.join(out_dir, f"cleaned_{base}")
    os.makedirs(out_dir, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    return {"in": path, "out": out_path, "original": original_count, "cleaned": len(deduped)}


def find_input_files(root_dir: str) -> List[str]:
    pattern = os.path.join(root_dir, "reddit_*.json")
    return glob.glob(pattern)


def main(argv: Optional[List[str]] = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]

    # default repo-root data/processed
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    default_in_dir = os.path.join(repo_root, "data", "processed")

    if len(argv) >= 1 and os.path.isfile(argv[0]):
        files = [argv[0]]
    else:
        files = find_input_files(default_in_dir)

    if not files:
        print(f"No input files found in {default_in_dir}. Provide a file path as the first argument.")
        return 1

    for p in files:
        try:
            info = process_file(p)
            print(f"Processed {info['in']} -> {info['out']} (original={info['original']}, cleaned={info['cleaned']})")
        except Exception as exc:
            print(f"Error processing {p}: {exc}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
