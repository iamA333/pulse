"""embedding.py

Batch embedding pipeline using sentence-transformers. Reads cleaned JSON files from
data/processed, encodes text in batches, and writes embeddings as .npy with a
corresponding metadata JSON file listing ids and texts.

Usage:
    python -m backend.pulse.pipelines.embedding
"""

from __future__ import annotations

import json
import os
import glob
from typing import List, Dict, Any

import numpy as np
from sentence_transformers import SentenceTransformer
from tqdm import tqdm


MODEL_NAME = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
BATCH_SIZE = int(os.getenv("EMBED_BATCH_SIZE", "64"))


def load_texts_from_cleaned(cleaned_dir: str) -> List[Dict[str, Any]]:
    files = glob.glob(os.path.join(cleaned_dir, "cleaned_reddit_*.json"))
    records = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
            for p in payload.get("posts", []):
                text = (p.get("title") or "") + "\n\n" + (p.get("selftext") or "")
                records.append({"id": p.get("id"), "text": text, "meta": p})
    return records


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def run(cleaned_dir: str, out_dir: str, model_name: str = MODEL_NAME, batch_size: int = BATCH_SIZE):
    print(f"Loading model {model_name}...")
    model = SentenceTransformer(model_name)

    records = load_texts_from_cleaned(cleaned_dir)
    if not records:
        print("No records found to embed.")
        return

    ensure_dir(out_dir)
    texts = [r["text"] for r in records]
    ids = [r["id"] for r in records]

    embeddings = []
    for i in tqdm(range(0, len(texts), batch_size), desc="Embedding batches"):
        batch_texts = texts[i : i + batch_size]
        emb = model.encode(batch_texts, show_progress_bar=False)
        embeddings.append(emb)

    embeddings = np.vstack(embeddings)

    # Save embeddings and metadata
    emb_path = os.path.join(out_dir, "embeddings.npy")
    meta_path = os.path.join(out_dir, "metadata.json")
    np.save(emb_path, embeddings)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({"ids": ids}, f, ensure_ascii=False, indent=2)

    print(f"Saved embeddings to {emb_path} and metadata to {meta_path}")


def main():
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    cleaned_dir = os.path.join(repo_root, "data", "processed")
    out_dir = os.path.join(repo_root, "data", "embeddings")
    run(cleaned_dir, out_dir)


if __name__ == "__main__":
    main()
