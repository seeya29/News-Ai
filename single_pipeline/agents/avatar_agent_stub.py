import os
import json
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import time

from ..logging_utils import PipelineLogger, StageLogger


class AvatarAgentStub:
    """Stub avatar agent designed to be provider-pluggable.

    Short-term: writes JSON metadata per video to data/avatar.
    Long-term: integrate with D-ID or HeyGen to render MP4.
    """

    def __init__(
        self,
        style: str = "anchor_english_1",
        logger: Optional[PipelineLogger] = None,
        output_base: Optional[str] = None,
        retention_days: int = 30,
        preset_map: Optional[Dict[str, str]] = None,
    ):
        self.style = style
        self.log = logger or PipelineLogger(component="avatar_stub")
        self.output_base = output_base or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "avatar"))
        self.retention_days = retention_days
        self.preset_map = preset_map or {
            "en|news": "anchor_english_1",
            "hi|news": "anchor_hindi_1",
            "en|youth": "creator_youth_1",
            "hi|devotional": "devotional_guru_1",
        }

    def _hash_id(self, title: str, key: str) -> str:
        h = hashlib.sha256((title + "|" + key).encode("utf-8", errors="ignore")).hexdigest()
        return f"article_{h[:12]}"

    def _cleanup_old(self) -> None:
        try:
            os.makedirs(self.output_base, exist_ok=True)
            cutoff = time.time() - (self.retention_days * 24 * 3600)
            for fname in os.listdir(self.output_base):
                fpath = os.path.join(self.output_base, fname)
                try:
                    if os.path.isfile(fpath) and os.path.getmtime(fpath) < cutoff:
                        os.remove(fpath)
                except Exception:
                    pass
        except Exception:
            pass

    def render(self, voice_items: List[Dict[str, Any]], category: str = "general") -> List[Dict[str, Any]]:
        run = StageLogger(source="pipeline", category=category, meta={"stage": "avatar", "style": self.style})
        run.start("avatar")
        self._cleanup_old()
        outputs: List[Dict[str, Any]] = []
        for v in voice_items:
            title = v.get("title") or "Untitled"
            lang = (v.get("lang") or "en").lower()
            tone = "news"
            preset = self.preset_map.get(f"{lang}|{tone}", self.style)
            audio_url = v.get("audio_url")
            # Metadata file instead of real video for now
            article_id = self._hash_id(title, audio_url or title)
            fname = f"{article_id}_{lang}_{tone}.json"
            os.makedirs(self.output_base, exist_ok=True)
            meta_path = os.path.join(self.output_base, fname)
            meta = {
                "title": title,
                "lang": lang,
                "tone": tone,
                "style_preset": preset,
                "audio_url": audio_url,
                "output": {
                    "format": "mp4",
                    "resolution": "1280x720",
                    "max_duration_seconds": 180,
                    "status": "stub",
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "category": category,
            }
            try:
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.log.warning("avatar_write_meta_failed", file=meta_path, error=str(e))
            # Serve via FastAPI static mount at /data/avatar
            video_url = f"/data/avatar/{fname}"
            outputs.append({
                "title": title,
                "lang": lang,
                "style": preset,
                "video_url": video_url,
                "metadata_path": meta_path,
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "category": category,
                },
            })
        run.complete("avatar", meta={"count": len(outputs)})
        run.end_run("completed")
        self.log.info("avatar_rendered", count=len(outputs))
        return outputs