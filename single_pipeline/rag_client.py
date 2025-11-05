import json
import os
import hashlib
from typing import Any, Dict, List, Tuple, Optional

from logging_utils import PipelineLogger

try:
    from providers.embeddings.adapter import EmbeddingLocalAdapter
except Exception:
    EmbeddingLocalAdapter = None


class RAGClient:
    """Initial RAG + dedup using local JSON cache.

    Stores recent stories in `output/rag_cache.json` with hashes.
    Provides `is_duplicate(title, body)` and lightweight `search` by title keywords.
    """

    def __init__(self, cache_path: Optional[str] = None, logger: Optional[PipelineLogger] = None):
        base = os.path.dirname(__file__)
        out_dir = os.path.join(base, "output")
        if not os.path.exists(out_dir):
            try:
                os.makedirs(out_dir, exist_ok=True)
            except Exception:
                # If output dir can't be created, log and continue with in-memory cache
                if logger:
                    logger.log_event("rag", {"warning": "failed_to_create_output_dir", "path": out_dir})
        self.cache_path = cache_path or os.path.join(out_dir, "rag_cache.json")
        self.cache: List[Dict[str, Any]] = []
        self.logger = logger or PipelineLogger()
        # Optional embedding adapter
        provider_choice = os.getenv("EMBED_PROVIDER", "").lower()
        self.embedder = EmbeddingLocalAdapter() if (provider_choice == "local" and EmbeddingLocalAdapter) else None
        self._load()

    def _load(self):
        try:
            if os.path.exists(self.cache_path):
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
        except Exception as e:
            # Fall back to empty cache but make noise for debugging
            self.logger.log_event("rag", {"error": "cache_load_failed", "detail": str(e), "path": self.cache_path})
            self.cache = []

    def _save(self):
        try:
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            # Log save failures instead of silently ignoring them
            self.logger.log_event("rag", {"error": "cache_save_failed", "detail": str(e), "path": self.cache_path})

    def _hash(self, title: str, body: str) -> str:
        h = hashlib.sha256((title + "\n" + body).encode("utf-8")).hexdigest()
        return h

    def is_duplicate(self, title: str, body: str, threshold: float = 0.92) -> bool:
        """Dedup using hash/token overlap; optionally embedding cosine if enabled.

        Hash equality -> duplicate.
        If embedder enabled, use cosine similarity; else fallback to token-overlap.
        """
        h = self._hash(title, body)
        text = (title + "\n" + body)
        current_vec: List[float] = []
        if self.embedder:
            try:
                current_vec = self.embedder.embed(text)
            except Exception as e:
                self.logger.log_event("rag", {"error": "embed_failed", "detail": str(e)})
                current_vec = []

        for item in self.cache:
            if item.get("hash") == h:
                return True
            if self.embedder and item.get("embedding"):
                try:
                    sim = self.embedder.cosine(current_vec, item.get("embedding", []))
                except Exception as e:
                    self.logger.log_event("rag", {"error": "cosine_failed", "detail": str(e)})
                    sim = 0.0
                if sim >= threshold:
                    return True
            else:
                # quick token-overlap fallback
                score = self._token_overlap((title + " " + body), (item.get("title", "") + " " + item.get("body", "")))
                if score >= threshold:
                    return True
        # not duplicate; append with embedding if available
        entry = {"hash": h, "title": title, "body": body}
        if self.embedder and current_vec:
            entry["embedding"] = current_vec
        self.cache.append(entry)
        self._save()
        return False

    def _token_overlap(self, a: str, b: str) -> float:
        ta = set(a.lower().split())
        tb = set(b.lower().split())
        if not ta or not tb:
            return 0.0
        inter = len(ta & tb)
        union = len(ta | tb)
        return inter / union

    def search(self, query: str, top_k: int = 3) -> List[Dict[str, Any]]:
        """Keyword-based search in cached titles/bodies."""
        q = set(query.lower().split())
        scored: List[Tuple[float, Dict[str, Any]]] = []
        for item in self.cache:
            score = self._token_overlap(" ".join(q), (item.get("title", "") + " " + item.get("body", "")).lower())
            if score > 0:
                scored.append((score, item))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [it for _, it in scored[:top_k]]