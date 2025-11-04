from typing import Any, Dict, List, Optional
import os

from logging_utils import PipelineLogger
from rag_client import RAGClient
try:
    from uniguru_client import UniguruClient
except Exception:
    UniguruClient = None

try:
    from providers.uniguru_local.adapter import UniguruLocalAdapter
except Exception:
    UniguruLocalAdapter = None


class FilterAgent:
    def __init__(self):
        self.rag = RAGClient()
        provider_choice = os.getenv("UNIGURU_PROVIDER", "").lower()
        if provider_choice == "local" and UniguruLocalAdapter:
            self.uniguru = UniguruLocalAdapter()
        elif UniguruClient:
            self.uniguru = UniguruClient()
        else:
            self.uniguru = None

    def _basic_language_detect(self, text: str) -> str:
        # Lightweight heuristic for English/Hindi/Tamil/Bengali detection
        total = len(text) or 1
        ascii_count = sum(1 for c in text if ord(c) < 128)
        devanagari_count = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)  # Hindi
        bengali_count = sum(1 for c in text if 0x0980 <= ord(c) <= 0x09FF)     # Bengali
        tamil_count = sum(1 for c in text if 0x0B80 <= ord(c) <= 0x0BFF)       # Tamil

        ratios = {
            "en": ascii_count / total,
            "hi": devanagari_count / total,
            "bn": bengali_count / total,
            "ta": tamil_count / total,
        }
        # Pick dominant script if clearly present; else mixed
        lang, score = max(ratios.items(), key=lambda x: x[1])
        return lang if score >= 0.2 else "mixed"

    def filter_items(self, items: List[Dict[str, Any]], logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for it in items:
            body = it.get("body", "")
            lang = self._basic_language_detect(body)
            dedup_key = it.get("title", "") + "::" + str(it.get("timestamp"))

            # Optional: tag category/tone/audience via Uniguru if available
            tags: Dict[str, Any] = {"category": None, "tone": None, "audience": "general"}
            if self.uniguru:
                try:
                    tags = self.uniguru.tag_text(title=it.get("title", "Untitled"), body=body, language=lang)
                except Exception:
                    tags = {"category": None, "tone": None, "audience": "general"}

            # Initial RAG-based dedup/context check
            dedup_flag = False
            try:
                dedup_flag = self.rag.is_duplicate(it.get("title", "Untitled"), body)
            except Exception:
                dedup_flag = False

            filtered.append({
                "title": it.get("title", "Untitled"),
                "body": body,
                "timestamp": it.get("timestamp"),
                "lang": lang,
                "category": tags.get("category"),
                "tone": tags.get("tone"),
                "audience": tags.get("audience"),
                "dedup_flag": dedup_flag,
                "dedup_key": dedup_key,
                "raw": it.get("raw", {})
            })
        if logger:
            logger.log_event("filter", {"count": len(filtered)})
        return filtered