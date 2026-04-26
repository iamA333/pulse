import sys
import json
from pathlib import Path

# Ensure package imports work when running the script directly
sys.path.append(str(Path(__file__).resolve().parents[1]))

from services.scoring import compute_score
from services.llm import summarize_cluster, generate_label


def main():
    # repo-root data/processed
    cleaned_path = Path(__file__).resolve().parents[3] / "data" / "processed" / "cleaned_reddit_MachineLearning.json"
    if not cleaned_path.exists():
        print(f"Cleaned file not found at {cleaned_path}")
        return 2

    data = json.loads(cleaned_path.read_text(encoding="utf-8"))
    posts = data.get("posts", [])[:10]
    cluster = {"posts": posts}

    score = compute_score(cluster)
    summary = summarize_cluster(posts)
    label = generate_label(posts)

    print("Score:", score)
    print("Label:", label)
    print("Summary:\n", summary)


if __name__ == "__main__":
    raise SystemExit(main())
