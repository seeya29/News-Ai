from typing import Any, Dict, List, Optional
import os
import hashlib
from datetime import datetime, timezone

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

    def _validate_item(self, it: Dict[str, Any], logger: Optional[PipelineLogger]) -> Optional[Dict[str, Any]]:
        # Ensure required fields exist and are strings
        title = it.get("title")
        body = it.get("body")
        
        if not title or not isinstance(title, str) or not title.strip():
            if logger:
                logger.warning("invalid_item_rejected", reason="missing_title")
            return None
            
        if not body or not isinstance(body, str) or not body.strip():
             if logger:
                logger.warning("invalid_item_rejected", reason="missing_body")
             return None

        timestamp = it.get("timestamp")
        return {**it, "title": title, "body": body, "timestamp": timestamp}

    def filter_items(self, items: List[Dict[str, Any]], logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        filtered: List[Dict[str, Any]] = []
        for it in items:
            valid_it = self._validate_item(it, logger)
            if not valid_it:
                # If we were processing raw inputs that needed a status return, we would return rejected item.
                # However, FilterAgent usually takes raw feed items and outputs clean items.
                # The contract says we should have an id and status.
                # Since we can't even generate an ID without title/body, we skip/reject completely 
                # OR we generate a placeholder ID if possible to track the rejection.
                # But without title/body, we can't guarantee uniqueness.
                # For now, we'll log rejection and skip adding to filtered list (filtering IS the rejection).
                continue
            
            it = valid_it
            title = it.get("title", "Untitled")
            body = it.get("body", "")
            timestamp = it.get("timestamp")

            # Language detection
            lang = self._basic_language_detect(body)

            # ID generation (hash of title+body)
            raw_id_str = f"{title}{body}"
            id_val = hashlib.md5(raw_id_str.encode("utf-8")).hexdigest()

            dedup_key = id_val
            try:
                # Use RAG to assign a semantic group key if possible
                dedup_key = self.rag.assign_group_key(
                    title=title,
                    body=body,
                    published_at_iso=timestamp,
                    category=None
                )
            except Exception:
                pass

            # Optional: tag category/tone/audience via Uniguru if available
            tags: Dict[str, Any] = {"category": None, "tone": "neutral", "audience": "general"}
            if self.uniguru:
                try:
                    tags = self.uniguru.tag_text(title=title, body=body, language=lang)
                except Exception as e:
                    if logger:
                        logger.warning("uniguru_tagging_failed", error=str(e))
                    tags = {"category": None, "tone": "neutral", "audience": "general"}
            
            tone = tags.get("tone") or "neutral"

            # Initial RAG-based dedup/context check
            dedup_flag = False
            try:
                dedup_flag = self.rag.is_duplicate(title, body)
            except Exception as e:
                if logger:
                    logger.warning("rag_dedup_failed", error=str(e))
                dedup_flag = False

            # Schema-compliant object
            # Note: 'body' is preserved for ScriptGen but is not part of the final contract schema
            item = {
                "id": id_val,
                "script": {
                    "text": "",
                    "headline": title,
                    "bullets": []
                },
                "tone": tone,
                "language": lang,
                "priority_score": 0.5,
                "trend_score": 0.5,
                "audio_path": None,
                "video_path": None,
                "stage_status": {
                    "fetch": "success",
                    "filter": "success",
                    "script": "pending",
                    "voice": "pending",
                    "avatar": "pending"
                },
                "timestamps": {
                    "fetched_at": timestamp,
                    "processed_at": datetime.now(timezone.utc).isoformat(),
                    "completed_at": None
                },
                # Internal fields
                "body": body,
                "dedup_flag": dedup_flag,
                "dedup_key": dedup_key,
                "raw": it.get("raw", {})
            }
            filtered.append(item)

        if logger:
            logger.info("filter_items_count", count=len(filtered))
        return filtered