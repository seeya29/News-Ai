import os
import json
import hashlib
import shutil
import subprocess
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import time

from ..logging_utils import PipelineLogger, StageLogger
try:
    import httpx  # type: ignore
except Exception:
    httpx = None


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
        self.provider = (os.getenv("AVATAR_PROVIDER") or "ffmpeg").lower()
        self.overlay_image_path = os.getenv("AVATAR_OVERLAY_IMAGE")
        self.did_api_key = os.getenv("DID_API_KEY")
        self.did_talk_api_url = os.getenv("DID_TALK_API_URL", "https://api.d-id.com/v1/talks")
        self.did_source_url = os.getenv("DID_SOURCE_URL")
        self.heygen_api_key = os.getenv("HEYGEN_API_KEY")
        self.public_base_url = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000")
        _hg_base = os.getenv("HEYGEN_API_BASE_URL") or os.getenv("HEYGEN_API_BASE") or "https://api.heygen.com"
        self.heygen_api_root = _hg_base.rstrip("/")
        self.heygen_avatar_id = os.getenv("HEYGEN_AVATAR_ID")
        self.heygen_voice_id = os.getenv("HEYGEN_VOICE_ID")
        self.sadtalker_root = os.getenv("SADTALKER_ROOT")
        self.sadtalker_source_image = os.getenv("SADTALKER_SOURCE_IMAGE")
        self.sadtalker_output_dir = os.getenv("SADTALKER_OUTPUT_DIR")

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
            cmd = ["ffmpeg", "-y", "-f", "lavfi", "-i", f"color=c={self.bg_color}:s={self.resolution}:d={duration:.2f}"]
            overlay = self.overlay_image_path if (self.overlay_image_path and os.path.isfile(self.overlay_image_path)) else None
            if overlay:
                cmd += ["-loop", "1", "-i", overlay]
            if audio_path and os.path.isfile(audio_path):
                cmd += ["-i", audio_path, "-shortest"]
            if overlay and (audio_path and os.path.isfile(audio_path)):
                cmd += [
                    "-filter_complex", "[0:v][1:v]overlay=(W-w)/2:(H-h)/2:format=auto[v]",
                    "-map", "[v]",
                    "-map", "2:a",
                ]
            elif overlay:
                cmd += [
                    "-filter_complex", "[0:v][1:v]overlay=(W-w)/2:(H-h)/2:format=auto[v]",
                    "-map", "[v]",
                ]
            cmd += ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", out_path]
            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return os.path.isfile(out_path)
        except Exception as e:
            self.log.warning("avatar_ffmpeg_failed", error=str(e))
            return False

    def _render_via_did(self, audio_path: Optional[str], out_path: str) -> bool:
        try:
            if not (httpx and self.did_api_key and self.did_source_url and audio_path and os.path.isfile(audio_path)):
                return False
            headers = {"Authorization": f"Bearer {self.did_api_key}"}
            with open(audio_path, "rb") as f:
                files = {"audio": (os.path.basename(audio_path), f, "audio/wav")}
                data = {"source_url": self.did_source_url}
                r = httpx.post(self.did_talk_api_url, headers=headers, files=files, data=data, timeout=60)
            if r.status_code >= 300:
                return False
            info = r.json()
            tid = str(info.get("id") or "")
            if not tid:
                return False
            poll_url = self.did_talk_api_url.rstrip("/") + "/" + tid
            for _ in range(60):
                pr = httpx.get(poll_url, headers=headers, timeout=30)
                if pr.status_code >= 300:
                    return False
                pj = pr.json()
                st = str(pj.get("status") or "")
                if st.lower() in ("done", "completed"):
                    res = pj.get("result_url") or pj.get("url")
                    if not res:
                        return False
                    vr = httpx.get(str(res), timeout=None)
                    if vr.status_code >= 300:
                        return False
                    with open(out_path, "wb") as ofp:
                        ofp.write(vr.content)
                    return os.path.isfile(out_path)
                if st.lower() in ("error", "failed"):
                    return False
                time.sleep(2.0)
            return False
        except Exception as e:
            self.log.warning("avatar_did_failed", error=str(e))
            return False

    def _public_url(self, route_or_path: Optional[str]) -> Optional[str]:
        try:
            if not route_or_path:
                return None
            p = str(route_or_path).replace("\\", "/")
            if p.startswith("/"):
                return self.public_base_url.rstrip("/") + p
            return self.public_base_url.rstrip("/") + "/" + p
        except Exception:
            return None

    def _render_via_heygen(self, audio_route: Optional[str], out_path: str) -> bool:
        try:
            if not (httpx and self.heygen_api_key and self.heygen_avatar_id and audio_route):
                return False
            headers = {
                "X-Api-Key": self.heygen_api_key,
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            gen_url = self.heygen_api_root + "/v2/video/generate"
            audio_url = self._public_url(audio_route)
            payload = {
                "test": False,
                "video_inputs": [
                    {
                        "character": {
                            "type": "avatar",
                            "avatar_id": self.heygen_avatar_id,
                        },
                        "voice": {
                            "type": "audio",
                            "audio_url": audio_url,
                        },
                    }
                ],
                "aspect_ratio": "16:9",
                "dimension": {"width": 1280, "height": 720},
            }
            r = httpx.post(gen_url, headers=headers, json=payload, timeout=60)
            if r.status_code >= 300:
                return False
            info = r.json()
            vid = str((info.get("data") or {}).get("video_id") or info.get("video_id") or "")
            if not vid:
                return False
            poll_url = self.heygen_api_root + "/v1/video_status.get"
            for _ in range(90):
                pr = httpx.get(poll_url, headers=headers, params={"video_id": vid}, timeout=30)
                if pr.status_code >= 300:
                    return False
                pj = pr.json()
                data = pj.get("data") or {}
                st = str(data.get("status") or pj.get("status") or "")
                if st.lower() in ("completed", "done"):
                    vurl = data.get("video_url") or pj.get("video_url") or pj.get("url")
                    if not vurl:
                        return False
                    vr = httpx.get(str(vurl), timeout=None)
                    if vr.status_code >= 300:
                        return False
                    with open(out_path, "wb") as ofp:
                        ofp.write(vr.content)
                    return os.path.isfile(out_path)
                if st.lower() in ("error", "failed"):
                    return False
                time.sleep(2.0)
            return False
        except Exception as e:
            self.log.warning("avatar_heygen_failed", error=str(e))
            return False

    def _render_via_local(self, source_image: Optional[str], audio_path: Optional[str], out_path: str) -> bool:
        try:
            root = self.sadtalker_root
            if not (root and os.path.isdir(root)):
                return False
            inf = os.path.join(root, "inference.py")
            if not os.path.isfile(inf):
                return False
            if not (source_image and os.path.isfile(source_image)):
                return False
            if not (audio_path and os.path.isfile(audio_path)):
                return False
            out_dir = self.sadtalker_output_dir or os.path.join(root, "outputs")
            os.makedirs(out_dir, exist_ok=True)
            py = os.getenv("SADTALKER_PYTHON")
            if not py:
                venv_py = os.path.join(root, "venv", "Scripts", "python.exe")
                if os.path.isfile(venv_py):
                    py = venv_py
                else:
                    py = "python"
            
            # Ensure paths are absolute and quoted for safety (though subprocess handles quoting)
            source_image_abs = os.path.abspath(source_image)
            audio_path_abs = os.path.abspath(audio_path)
            out_dir_abs = os.path.abspath(out_dir)
            
            # SadTalker inference command
            # Note: Removed --still as it might cause static face issues if not handled correctly by the model
            cmd = [
                py,
                inf,
                "--source_image", source_image_abs,
                "--driven_audio", audio_path_abs,
                "--result_dir", out_dir_abs,
                "--preprocess", "full",  # changed from 'crop' to 'full' to avoid cropping issues
            ]
            ckpt = os.getenv("SADTALKER_CHECKPOINTS")
            if ckpt and os.path.isdir(os.path.dirname(ckpt)):
                cmd += ["--checkpoint_dir", ckpt]
            p = subprocess.run(cmd, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if p.returncode != 0:
                err_msg = p.stderr.decode(errors="ignore")
                self.log.error("avatar_local_command_failed", returncode=p.returncode, error=err_msg)
                raise RuntimeError(err_msg)
            mp4s: List[str] = []
            try:
                for f in os.listdir(out_dir):
                    p = os.path.join(out_dir, f)
                    if os.path.isfile(p) and f.lower().endswith(".mp4"):
                        mp4s.append(p)
                mp4s.sort(key=lambda p: os.path.getmtime(p))
            except Exception:
                pass
            if not mp4s:
                return False
            src = mp4s[-1]
            shutil.copyfile(src, out_path)
            return os.path.isfile(out_path)
        except Exception as e:
            self.log.warning("avatar_local_failed", error=str(e))
            return False

    def render(self, voice_items: List[Dict[str, Any]], category: str = "general") -> List[Dict[str, Any]]:
        run = StageLogger(source="pipeline", category=category, meta={"stage": "avatar", "style": self.style})
        run.start("avatar")
        self._cleanup_old()
        
        # Default overlay if not set
        if not self.overlay_image_path:
            potential_assets = [
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets", "source_image.jpg")),
                os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "assets", "source_image.png"))
            ]
            for p in potential_assets:
                if os.path.isfile(p):
                    self.overlay_image_path = p
                    break

        outputs: List[Dict[str, Any]] = []
        for v in voice_items:
            title = v.get("title") or v.get("script", {}).get("headline") or "Untitled"
            lang = (v.get("lang") or "en").lower()
            tone = "news"
            preset = self.preset_map.get(f"{lang}|{tone}", self.style)
            audio_url = v.get("audio_url")
            
            # Use existing ID if available to ensure consistency
            article_id = v.get("id") or self._hash_id(title, audio_url or title)
            base = f"{article_id}_{lang}_{tone}"
            os.makedirs(self.output_base, exist_ok=True)
            meta_path = os.path.join(self.output_base, f"{base}.json")
            audio_path = v.get("audio_path")
            duration = self._audio_duration_seconds(audio_path)
            mp4_path = os.path.join(self.output_base, f"{base}.mp4")
            rendered = False
            if self.provider == "did":
                rendered = self._render_via_did(audio_path, mp4_path)
            elif self.provider == "heygen":
                rendered = self._render_via_heygen(audio_url, mp4_path)
            elif self.provider == "local":
                src_img = self.sadtalker_source_image or self.overlay_image_path
                rendered = self._render_via_local(src_img, audio_path, mp4_path)
            elif self.provider == "ffmpeg":
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
