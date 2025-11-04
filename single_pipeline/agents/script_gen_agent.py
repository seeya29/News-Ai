from typing import Any, Dict, List, Optional

from logging_utils import PipelineLogger


DEFAULT_STYLE = "Professional newsroom tone, clear and concise."
STYLE_VARIANTS = {
    "formal": "Formal newsroom, objective voice, structured paragraphs.",
    "kids": "Simple words, friendly tone, short sentences for kids.",
    "youth": "Casual, energetic, relatable examples for youth.",
    "devotional": "Respectful, uplifting, values-focused tone.",
}


class ScriptGenAgent:
    def __init__(self):
        pass

    def _compose_script(self, title: str, body: str, tone: str = "neutral", audience: str = "general") -> str:
        style = STYLE_VARIANTS.get(tone, DEFAULT_STYLE)
        return (
            f"[Style: {style} | Audience: {audience}]\n"
            f"Headline: {title}\n\n"
            f"Story: {body}\n"
        )

    def generate_scripts(self, items: List[Dict[str, Any]], logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        scripts: List[Dict[str, Any]] = []
        for it in items:
            title = it.get("title", "Untitled")
            body = it.get("body", "")
            audience = it.get("audience", "general")
            tone = (it.get("tone") or "neutral").lower()
            script_text = self._compose_script(title, body, tone, audience)
            scripts.append({
                "title": title,
                "script": script_text,
                "lang": it.get("lang", "en"),
                "tone": tone,
                "audience": audience,
                "timestamp": it.get("timestamp"),
                "raw": it
            })
        if logger:
            logger.log_event("script", {"count": len(scripts)})
        return scripts