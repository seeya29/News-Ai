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
        t_key = (tone or "neutral").lower()
        if t_key not in tone_map:
             self.log.warning("unknown_tone_fallback", tone=tone, fallback="neutral")
             t_key = "neutral"
             
        prefix = tone_map[t_key]
        return f"{prefix} {title.strip()} — {body.strip()[:400]}"

    def _style_variant(self, title: str, body: str, style: str) -> str:
        s = style.lower()
        if s == "formal":
            return f"In today’s bulletin: {title.strip()}. Key details: {body.strip()[:400]}"
        if s == "kids":
            return f"Story time! {title.strip()}. In simple words: {body.strip()[:300]}"
        if s == "youth":
            return f"Fast update: {title.strip()} — {body.strip()[:320]}"
        if s == "devotional":
            return f"With grace and calm, {title.strip()}. Reflection: {body.strip()[:350]}"
        return f"{title.strip()} — {body.strip()[:400]}"

    def generate(self, filtered_items: List[Dict[str, Any]], category: str = "general") -> List[Dict[str, Any]]:
        run = StageLogger(source="pipeline", category=category, meta={"stage": "script_gen"})
        run.start("summarize")
        scripts: List[Dict[str, Any]] = []
        for it in filtered_items:
            # Extract fields from new schema + body
            title = it.get("title") or it.get("script", {}).get("headline") or "Untitled"
            body = it.get("body") or ""
            tone = it.get("tone")
            lang = it.get("language") or "en"
            
            # Generate script components
            try:
                bullets = self._bullets(body)
                headline = self._headline(title)
                text = self._conversational(title, body, tone, "general")
                status = "success"
                
                # Check for empty script (failure mode)
                if not text.strip():
                    status = "failed"
                    # We might still want to pass it through as failed?
                    # Or maybe "rejected" if content was empty.
            except Exception as e:
                self.log.error("script_gen_failed", error=str(e))
                status = "failed"
                text = ""
                headline = ""
                bullets = []
            
            # Update item
            new_item = it.copy()
            new_item["script"] = {
                "text": text,
                "headline": headline,
                "bullets": bullets
            }
            # Update status and timestamps
            if "stage_status" not in new_item:
                new_item["stage_status"] = {}
            new_item["stage_status"]["script"] = status
            
            if "timestamps" not in new_item:
                new_item["timestamps"] = {}
            new_item["timestamps"]["processed_at"] = datetime.now(timezone.utc).isoformat()
            
            scripts.append(new_item)
            
        run.complete("summarize", meta={"count": len(scripts)})
        run.end_run("completed")
        self.log.info("scripts_generated", count=len(scripts))
        return scripts
