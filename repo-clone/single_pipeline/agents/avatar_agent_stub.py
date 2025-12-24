import os
import json
import hashlib
import shutil
import subprocess
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
        self.bg_color = os.getenv("AVATAR_BG_COLOR", "#0B1F3A")
        self.resolution = os.getenv("AVATAR_RESOLUTION", "1280x720")

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

    def _audio_duration_seconds(self, audio_path: Optional[str]) -> float:
        try:
            if not audio_path or not os.path.isfile(audio_path):
                return 0.0
            import wave
            with wave.open(audio_path, "rb") as wf:
                frames = wf.getnframes()
                rate = wf.getframerate() or 16000
                return max(0.0, float(frames) / float(rate))
        except Exception:
            return 0.0

    def _ffmpeg_available(self) -> bool:
        exe = shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
        return bool(exe)

    def _render_static_video(self, audio_path: Optional[str], out_path: str, duration: float) -> bool:
        try:
            if not self._ffmpeg_available():
                return False
            if duration <= 0.0:
                duration = 5.0
            cmd = [
                "ffmpeg",
                "-y",
                "-f", "lavfi",
                "-i", f"color=c={self.bg_color}:s={self.resolution}:d={duration:.2f}",
            ]
            if audio_path and os.path.isfile(audio_path):
                cmd += ["-i", audio_path, "-shortest"]
            cmd += ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", out_path]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return os.path.isfile(out_path)
        except Exception as e:
            self.log.warning("avatar_ffmpeg_failed", error=str(e))
            return False

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
            article_id = self._hash_id(title, audio_url or title)
            base = f"{article_id}_{lang}_{tone}"
            os.makedirs(self.output_base, exist_ok=True)
            meta_path = os.path.join(self.output_base, f"{base}.json")
            audio_path = v.get("audio_path")
            duration = self._audio_duration_seconds(audio_path)
            mp4_path = os.path.join(self.output_base, f"{base}.mp4")
            rendered = self._render_static_video(audio_path, mp4_path, duration)
            meta = {
                "title": title,
                "lang": lang,
                "tone": tone,
                "style_preset": preset,
                "audio_url": audio_url,
                "audio_path": audio_path,
                "output": {
                    "format": ("mp4" if rendered else "json"),
                    "resolution": self.resolution,
                    "duration_seconds": duration,
                    "status": ("rendered" if rendered else "stub"),
                },
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "category": category,
            }
            try:
                with open(meta_path, "w", encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.log.warning("avatar_write_meta_failed", file=meta_path, error=str(e))
            video_url = f"/data/avatar/{os.path.basename(mp4_path) if rendered else os.path.basename(meta_path)}"
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
