from typing import Any, Dict, List, Optional
import os

from ..logging_utils import PipelineLogger
from ..rag_client import RAGClient
try:
    from uniguru_client import UniguruClient
except Exception:
    UniguruClient = None

try:
    from ..providers.uniguru_local.adapter import UniguruLocalAdapter
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
        # Configurable language dominance threshold (default 0.3)
        try:
            self.lang_threshold = float(os.getenv("LANG_DOMINANCE_THRESHOLD", "0.3"))
        except Exception:
            self.lang_threshold = 0.3

    def _basic_language_detect(self, text: str) -> str:
        # Lightweight heuristic for common Indic scripts + English
        total = len(text) or 1
        ascii_count = sum(1 for c in text if ord(c) < 128)
        devanagari_count = sum(1 for c in text if 0x0900 <= ord(c) <= 0x097F)  # Hindi
        bengali_count = sum(1 for c in text if 0x0980 <= ord(c) <= 0x09FF)     # Bengali
        tamil_count = sum(1 for c in text if 0x0B80 <= ord(c) <= 0x0BFF)       # Tamil
        gurmukhi_count = sum(1 for c in text if 0x0A00 <= ord(c) <= 0x0A7F)    # Punjabi
        gujarati_count = sum(1 for c in text if 0x0A80 <= ord(c) <= 0x0AFF)    # Gujarati
        telugu_count = sum(1 for c in text if 0x0C00 <= ord(c) <= 0x0C7F)      # Telugu
        kannada_count = sum(1 for c in text if 0x0C80 <= ord(c) <= 0x0CFF)     # Kannada
        malayalam_count = sum(1 for c in text if 0x0D00 <= ord(c) <= 0x0D7F)   # Malayalam
        arabic_count = sum(1 for c in text if 0x0600 <= ord(c) <= 0x06FF)      # Urdu (Arabic script)

        ratios = {
            "en": ascii_count / total,
            "hi": devanagari_count / total,
            "bn": bengali_count / total,
            "ta": tamil_count / total,
            "pa": gurmukhi_count / total,
            "gu": gujarati_count / total,
            "te": telugu_count / total,
            "kn": kannada_count / total,
            "ml": malayalam_count / total,
            "ur": arabic_count / total,
        }
        # Pick dominant script if clearly present; else mixed
        lang, score = max(ratios.items(), key=lambda x: x[1])
        # Use stricter threshold for very short texts
        threshold = max(self.lang_threshold, 0.5) if total < 15 else self.lang_threshold
        # If nothing dominates even slightly, mark unknown; else mixed
        if score < max(0.05, self.lang_threshold):
            return "unknown"
        return lang if score >= threshold else "mixed"

    def _validate_item(self, it: Dict[str, Any], logger: Optional[PipelineLogger]) -> Dict[str, Any]:
        # Ensure required fields exist and are strings; coerce when possible
        title = it.get("title", "Untitled")
        body = it.get("body", "")
        if not isinstance(title, str):
            title = str(title)
        if not isinstance(body, str):
            body = str(body) if body is not None else ""
        timestamp = it.get("timestamp")
        if logger and (not title or not isinstance(title, str) or body is None):
            logger.warning("invalid_item", title_type=str(type(it.get("title"))), body_type=str(type(it.get("body"))))
        return {**it, "title": title, "body": body, "timestamp": timestamp}

    def filter_items(self, items: List[Dict[str, Any]], logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for it in items:
            it = self._validate_item(it, logger)
            body = it.get("body", "")
            lang = self._basic_language_detect(body)
            dedup_key = it.get("title", "") + "::" + str(it.get("timestamp"))
            try:
                # Use RAG to assign a semantic group key if possible
                dedup_key = self.rag.assign_group_key(
                    title=it.get("title", "Untitled"),
                    body=body,
                    published_at_iso=it.get("timestamp"),
                    category=None
                )
            except Exception:
                # Fallback to simple key
                pass

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
            logger.info("filter_items_count", count=len(filtered))
        return filtered