from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from ..logging_utils import PipelineLogger, StageLogger


class ScriptGenAgent:
    """Lightweight script generator.

    Produces multiple variants per item: bullet points, short headline script,
    and a conversational read suitable for TTS/Avatar. Tone and audience hints
    from FilterAgent are respected if present.
    """

    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.log = logger or PipelineLogger(component="script_gen")

    def _bullets(self, body: str) -> List[str]:
        # Naive bulletization: split by sentence-like chunks
        parts = [p.strip() for p in body.split(".") if p.strip()]
        return parts[:5]

    def _headline(self, title: str) -> str:
        return title.strip()[:140]

    def _conversational(self, title: str, body: str, tone: Optional[str], audience: Optional[str]) -> str:
        tone_map = {
            "formal": "In today’s update,",
            "casual": "Quick take:",
            "neutral": "Here’s what happened:",
        }
        prefix = tone_map.get((tone or "neutral").lower(), tone_map["neutral"])
        return f"{prefix} {title.strip()} — {body.strip()[:400]}"

    def generate(self, filtered_items: List[Dict[str, Any]], category: str = "general") -> List[Dict[str, Any]]:
        run = StageLogger(source="pipeline", category=category, meta={"stage": "script_gen"})
        run.start("summarize")
        scripts: List[Dict[str, Any]] = []
        for it in filtered_items:
            title = it.get("title") or "Untitled"
            body = it.get("body") or ""
            tone = it.get("tone")
            audience = it.get("audience") or "general"
            lang = it.get("lang") or "en"
            bullets = self._bullets(body)
            headline = self._headline(title)
            narration = self._conversational(title, body, tone, audience)
            scripts.append({
                "title": title,
                "lang": lang,
                "audience": audience,
                "tone": tone or "neutral",
                "variants": {
                    "bullets": bullets,
                    "headline": headline,
                    "narration": narration,
                },
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "category": category,
                },
            })
        run.complete("summarize", meta={"count": len(scripts)})
        run.end_run("completed")
        self.log.info("scripts_generated", count=len(scripts))
        return scripts