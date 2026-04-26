"""LLM service: summarization and labeling utilities.

This module is intentionally isolated from pipeline logic. It exposes small helpers
that accept post lists and return text. The LLM call is abstracted behind `llm_call`.
"""

from typing import Iterable, Dict, Any


def llm_call(prompt: str) -> str:
    """Placeholder LLM caller.

    Replace this with your preferred LLM integration (OpenAI, Anthropic, local LLM).
    Keep this function isolated so pipeline code doesn't depend on provider APIs.
    """
    # Minimal placeholder: echo the prompt head. In production, call the API here.
    return prompt[:200] + "..."


def summarize_cluster(posts: Iterable[Dict[str, Any]]) -> str:
    """Create a summarization prompt and return the LLM output."""
    texts = []
    for p in posts:
        t = (p.get("title") or "")
        texts.append(t)

    prompt = "Summarize the following posts into a short paragraph and list the main themes:\n\n"
    prompt += "\n".join(texts[:20])

    return llm_call(prompt)


def generate_label(posts: Iterable[Dict[str, Any]]) -> str:
    """Generate a short label for the cluster using an LLM call."""
    prompt = "Provide a concise 3-word label for the following posts:\n\n"
    titles = [p.get("title") for p in posts]
    prompt += "\n".join(titles[:20])
    return llm_call(prompt)


__all__ = ["summarize_cluster", "generate_label", "llm_call"]
