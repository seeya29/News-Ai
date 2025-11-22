import json
import os
import hashlib
import time
import tempfile
import threading
from typing import Any, Dict, List, Tuple, Optional

from .logging_utils import PipelineLogger

try:
    from .providers.embeddings.adapter import EmbeddingLocalAdapter
except Exception:
    EmbeddingLocalAdapter = None


class RAGClient:
    """Initial RAG + dedup using local JSON cache.

    Stores recent stories in `output/rag_cache.json` with hashes.
    Provides `is_duplicate(title, body)` and lightweight `search` by title keywords.
    """

    def __init__(self, cache_path: Optional[str] = None, logger: Optional[PipelineLogger] = None):
        base = os.path.dirname(__file__)
        preferred_out_dir = os.path.join(base, "output")
        self.logger = logger or PipelineLogger(component="rag_client")
        # Determine a writable output directory with fallback
        self.output_dir = self._resolve_output_dir(preferred_out_dir)
        # Cache path and persistence flags
        self.cache_path = cache_path or (os.path.join(self.output_dir, "rag_cache.json") if self.output_dir else None)
        self.persistence_enabled = bool(self.cache_path)
        # Simple inter-process lock via lock file and in-process lock
        self._lock_file_path = os.path.join(self.output_dir, ".rag_cache.lock") if self.output_dir else None
        self._process_lock = threading.Lock()
        self.cache: List[Dict[str, Any]] = []
        # Optional embedding adapter
        provider_choice = os.getenv("EMBED_PROVIDER", "").lower()
        self.embedder = EmbeddingLocalAdapter() if (provider_choice == "local" and EmbeddingLocalAdapter) else None
        self._load()

    def _resolve_output_dir(self, preferred_out_dir: str) -> Optional[str]:
        # Try preferred directory under repository
        try:
            os.makedirs(preferred_out_dir, exist_ok=True)
            return preferred_out_dir
        except Exception as e:
            self.logger.warning("failed_to_create_output_dir", path=preferred_out_dir, detail=str(e))
        # Fallback to temp directory
        fallback = os.path.join(tempfile.gettempdir(), "single_pipeline_output")
        try:
            os.makedirs(fallback, exist_ok=True)
            self.logger.info("using_temp_output_dir", path=fallback)
            return fallback
        except Exception as e:
            # Disable persistence if no directory is writable
            self.logger.error("no_writable_output_dir", preferred=preferred_out_dir, fallback=fallback, detail=str(e))
            return None

    def _load(self):
        if not self.persistence_enabled:
            self.cache = []
            self.logger.warning("persistence_disabled", reason="no_output_dir")
            return
        try:
            if self.cache_path and os.path.exists(self.cache_path):
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    self.cache = json.load(f)
        except Exception as e:
            # Fall back to empty cache but make noise for debugging
            self.logger.error("cache_load_failed", detail=str(e), path=str(self.cache_path))
            self.cache = []

    def _save(self):
        if not self.persistence_enabled or not self.cache_path:
            return
        # Use an in-process mutex + a best-effort inter-process lock file
        with self._process_lock:
            lock_acquired = self._acquire_file_lock(timeout=5.0)
            try:
                tmp_path = self.cache_path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
                # Atomic replace
                os.replace(tmp_path, self.cache_path)
            except Exception as e:
                self.logger.error("cache_save_failed", detail=str(e), path=str(self.cache_path))
            finally:
                if lock_acquired:
                    self._release_file_lock()

    def _acquire_file_lock(self, timeout: float = 5.0, poll_interval: float = 0.1) -> bool:
        if not self._lock_file_path:
            return False
        start = time.time()
        while True:
            try:
                # Exclusive creation fails if the file already exists
                fd = os.open(self._lock_file_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                return True
            except FileExistsError:
                if time.time() - start > timeout:
                    self.logger.warning("lock_timeout", lock_path=self._lock_file_path)
                    return False
                time.sleep(poll_interval)
            except Exception as e:
                # Any unexpected error -> don't block persistence, but log
                self.logger.warning("lock_failed", detail=str(e))
                return False

    def _release_file_lock(self):
        if not self._lock_file_path:
            return
        try:
            os.unlink(self._lock_file_path)
        except Exception:
            # Ignore errors on unlock
            pass

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

    # --------------------
    # Group key assignment for dedup
    # --------------------
    def _round_vec(self, vec: List[float], decimals: int = 3) -> List[float]:
        try:
            return [round(float(x), decimals) for x in vec]
        except Exception:
            return vec

    def assign_group_key(
        self,
        title: str,
        body: str,
        published_at_iso: Optional[str],
        category: Optional[str] = None,
        threshold: Optional[float] = None,
        window_secs: Optional[int] = None,
    ) -> str:
        """Return a stable group_key for a new item.

        Policy:
        - If an item in cache within time window is similar (cosine >= threshold), reuse its group_key.
        - Else, generate new group_key = sha256(rounded_vector + time_bucket).
        - If no embeddings provider, fall back to sha256(text_norm + time_bucket).

        Defaults:
        - threshold from env DEDUP_THRESHOLD (default 0.92)
        - window_secs from env GROUP_TIME_WINDOW (default 24h)
        """
        try:
            import datetime
            dt = datetime.datetime.fromisoformat((published_at_iso or "").replace("Z", "+00:00"))
        except Exception:
            dt = None
        threshold = float(os.getenv("DEDUP_THRESHOLD", "0.92")) if threshold is None else float(threshold)
        window_secs = int(os.getenv("GROUP_TIME_WINDOW", str(24 * 3600))) if window_secs is None else int(window_secs)

        # Prepare embedding or text
        text = (title or "") + "\n" + (body or "")
        vec: List[float] = []
        if self.embedder:
            try:
                vec = self.embedder.embed(text)
            except Exception as e:
                self.logger.error("embed_failed", detail=str(e))
                vec = []

        # Time bucket
        now_ts = time.time() if dt is None else dt.timestamp()
        bucket = int(now_ts // window_secs)

        # Try to reuse a group within the window
        for item in reversed(self.cache):  # check recent first
            its = float(item.get("ts") or 0.0)
            if window_secs > 0 and abs(now_ts - its) > window_secs:
                continue
            candidate_gk = item.get("group_key")
            if not candidate_gk:
                continue
            if self.embedder and vec and item.get("embedding"):
                try:
                    sim = self.embedder.cosine(vec, item.get("embedding") or [])
                except Exception as e:
                    self.logger.error("cosine_failed", detail=str(e))
                    sim = 0.0
                if sim >= threshold:
                    # Reuse this group
                    self._append_cache_entry(title, body, vec, now_ts, candidate_gk)
                    return candidate_gk
            else:
                # Fallback token overlap
                score = self._token_overlap(text, (item.get("title", "") + " " + item.get("body", "")))
                if score >= threshold:
                    self._append_cache_entry(title, body, vec, now_ts, candidate_gk)
                    return candidate_gk

        # Create new group key
        if vec:
            rounded = self._round_vec(vec, decimals=3)
            payload = json.dumps({"v": rounded, "b": bucket})
        else:
            payload = json.dumps({"t": (title or "")[:256], "b": bucket})
        gk = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
        self._append_cache_entry(title, body, vec, now_ts, gk)
        return gk

    def _append_cache_entry(self, title: str, body: str, vec: List[float], ts: float, group_key: str) -> None:
        entry = {"hash": self._hash(title, body), "title": title, "body": body, "ts": ts, "group_key": group_key}
        if self.embedder and vec:
            entry["embedding"] = vec
        self.cache.append(entry)
        self._save()