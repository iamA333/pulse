"""clustering.py

Load embeddings from data/embeddings/embeddings.npy and cluster using DBSCAN.
Writes cluster assignments to data/clusters/clusters.json with mapping id->cluster.

Usage:
    python -m backend.pulse.pipelines.clustering
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

import numpy as np
from sklearn.cluster import DBSCAN


def run(emb_dir: str, out_dir: str, eps: float = 0.5, min_samples: int = 5):
    emb_path = os.path.join(emb_dir, "embeddings.npy")
    meta_path = os.path.join(emb_dir, "metadata.json")
    if not os.path.exists(emb_path) or not os.path.exists(meta_path):
        print("Embeddings or metadata not found. Run embedding pipeline first.")
        return

    embeddings = np.load(emb_path)
    with open(meta_path, "r", encoding="utf-8") as f:
        meta = json.load(f)
    ids = meta.get("ids", [])

    print(f"Running DBSCAN on {len(embeddings)} embeddings (eps={eps}, min_samples={min_samples})")
    clusterer = DBSCAN(eps=eps, min_samples=min_samples, metric="euclidean")
    labels = clusterer.fit_predict(embeddings)

    mapping = {str(_id): int(label) for _id, label in zip(ids, labels)}

    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "clusters.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"clusters": mapping}, f, ensure_ascii=False, indent=2)

    print(f"Saved clusters to {out_path}")


def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    emb_dir = os.path.join(repo_root, "data", "embeddings")
    out_dir = os.path.join(repo_root, "data", "clusters")
    run(emb_dir, out_dir)


if __name__ == "__main__":
    main()
