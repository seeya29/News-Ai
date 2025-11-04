import os
import requests
from typing import Any, Dict


class UniguruClient:
    """Lightweight client to call Uniguru LLM tagging endpoint.

    Configure via env:
    - UNIGURU_BASE_URL (e.g., http://localhost:3000)
    - UNIGURU_TAG_PATH (default: /api/llm/tag)
    - UNIGURU_API_KEY (optional header Authorization: Bearer)
    """

    def __init__(self):
        base = os.getenv("UNIGURU_BASE_URL", "")
        path = os.getenv("UNIGURU_TAG_PATH", "/api/llm/tag")
        self.url = (base.rstrip("/") + path) if base else ""
        self.api_key = os.getenv("UNIGURU_API_KEY")

    def tag_text(self, title: str, body: str, language: str) -> Dict[str, Any]:
        if not self.url:
            # Fallback simple heuristic if endpoint not configured
            return {
                "category": "general",
                "tone": "neutral",
                "audience": "general",
            }
        payload = {"title": title, "body": body, "language": language}
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        r = requests.post(self.url, json=payload, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json() if hasattr(r, "json") else {}
        return {
            "category": data.get("category", "general"),
            "tone": data.get("tone", "neutral"),
            "audience": data.get("audience", "general"),
        }