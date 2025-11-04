from typing import Any, Dict


class UniguruLocalAdapter:
    def tag_text(self, title: str, body: str, language: str) -> Dict[str, Any]:
        # Placeholder implementation; replace with your local LLM logic.
        return {
            "category": "general",
            "tone": "neutral",
            "audience": "general",
        }