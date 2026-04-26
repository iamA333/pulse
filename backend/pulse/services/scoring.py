"""Scoring utilities for trend scoring engine.

These functions are kept isolated from pipeline logic and operate on an in-memory
`cluster` object with attribute `posts` (iterable of dicts). Each post dict is
expected to have keys like `id`, `title`, `selftext`, `created_iso`, `author`,
`subreddit` or `source`.
"""

from collections import Counter
from datetime import datetime
from typing import Iterable, Dict, Any


def _parse_created(dt_str: str):
    try:
        return datetime.fromisoformat(dt_str)
    except Exception:
        return None


def compute_growth(posts: Iterable[Dict[str, Any]]) -> float:
    """Compute a simple growth score in [0,1].

    Method: split timestamps into early and recent halves and compute the ratio
    recent_count / max(1, early_count) - 1, clamped to [0,1]. If no timestamps,
    fallback to 0.
    """
    times = []
    for p in posts:
        dt = p.get("created_iso") or p.get("created_utc")
        if isinstance(dt, (int, float)):
            try:
                times.append(datetime.fromtimestamp(float(dt)))
            except Exception:
                continue
        elif isinstance(dt, str):
            parsed = _parse_created(dt)
            if parsed:
                times.append(parsed)

    if len(times) < 2:
        return 0.0

    times.sort()
    mid = len(times) // 2
    early_count = mid
    recent_count = len(times) - mid

    if early_count == 0:
        return 1.0 if recent_count > 0 else 0.0

    ratio = recent_count / max(1, early_count)
    growth = max(0.0, min(1.0, ratio - 1.0))
    return growth


def compute_novelty(posts: Iterable[Dict[str, Any]]) -> float:
    """Compute novelty as fraction of unique titles (or URLs) -> [0,1]."""
    items = []
    for p in posts:
        title = (p.get("title") or "").strip().lower()
        if title:
            items.append(title)
        else:
            url = p.get("url")
            if url:
                items.append(url)

    if not items:
        return 0.0
    unique = len(set(items))
    return min(1.0, unique / len(items))


def compute_source_weight(posts: Iterable[Dict[str, Any]]) -> float:
    """Compute a weight based on the sources present in the cluster.

    Heuristic weights: reddit=1.0, x=1.1, newsletter=1.3; average across posts.
    """
    weights_map = {"reddit": 1.0, "x": 1.1, "twitter": 1.1, "newsletter": 1.3}
    vals = []
    for p in posts:
        src = (p.get("source") or p.get("subreddit") or "reddit").lower()
        # normalize possible reddit subreddit names
        if src.startswith("r/"):
            src = "reddit"
        vals.append(weights_map.get(src, 1.0))

    if not vals:
        return 1.0
    return sum(vals) / len(vals)


def compute_score(cluster: Dict[str, Any]) -> float:
    """Compute an overall trend score for a cluster.

    Formula (as requested):
        volume*0.5 + growth*0.3 + (novelty*0.2 * source_weight)

    Where:
    - volume: number of posts
    - growth: computed growth score in [0,1]
    - novelty: computed novelty in [0,1]
    - source_weight: multiplier >= 0
    """
    posts = list(cluster.get("posts", []))
    volume = len(posts)
    growth = compute_growth(posts)
    novelty = compute_novelty(posts)
    source_weight = compute_source_weight(posts)

    score = (volume * 0.5) + (growth * 0.3) + (novelty * 0.2 * source_weight)
    return float(score)


__all__ = [
    "compute_score",
    "compute_growth",
    "compute_novelty",
    "compute_source_weight",
]
