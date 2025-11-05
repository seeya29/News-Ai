from typing import Any, Dict, List, Optional

from logging_utils import PipelineLogger
from rag_client import RAGClient


DEFAULT_STYLE = "Professional newsroom tone, clear and concise."
STYLE_VARIANTS = {
    "formal": "Formal newsroom, objective voice, structured paragraphs.",
    "kids": "Simple words, friendly tone, short sentences for kids.",
    "youth": "Casual, energetic, relatable examples for youth.",
    "devotional": "Respectful, uplifting, values-focused tone.",
}


class ScriptGenAgent:
    def __init__(self, rag: Optional[RAGClient] = None):
        # Optional RAG client for context-aware enrichment
        self.rag = rag or RAGClient()

    def _build_context(self, title: str, body: str, top_k: int = 2) -> str:
        # Use title + body terms to search prior cache for related items
        results = []
        try:
            results = self.rag.search(f"{title} {body}", top_k=top_k)
        except Exception:
            results = []
        if not results:
            return ""
        lines: List[str] = []
        for idx, it in enumerate(results, start=1):
            ctx_title = it.get("title", "")[:140]
            ctx_body = it.get("body", "")[:220]
            lines.append(f"{idx}. {ctx_title} — {ctx_body}")
        return "\n".join(lines)

    def _compose_script(self, title: str, body: str, tone: str = "neutral", audience: str = "general") -> str:
        style = STYLE_VARIANTS.get(tone, DEFAULT_STYLE)
        context = self._build_context(title, body)
        context_block = f"\nRelated Context:\n{context}\n" if context else "\n"
        return (
            f"[Style: {style} | Audience: {audience}]\n"
            f"Headline: {title}\n\n"
            f"Story: {body}\n"
            f"{context_block}"
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
                "category": it.get("category"),
                "timestamp": it.get("timestamp"),
                "raw": it
            })
        if logger:
            logger.log_event("script", {"count": len(scripts), "context_enriched": True})
        return scripts