import os
import time
import wave
import struct
import hashlib
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from ..logging_utils import PipelineLogger, StageLogger
try:
    import pyttsx3  # type: ignore
except Exception:
    pyttsx3 = None

try:
    import pythoncom
except ImportError:
    pythoncom = None



class TTSAgentStub:
    """Stub TTS agent designed to be provider-pluggable.

    Short-term: writes WAV files locally for testing.
    Long-term: swap synthesize backend to Azure Cognitive Services or similar.
    """

    def __init__(
        self,
        voice: str = "en-US-GuyNeural",
        logger: Optional[PipelineLogger] = None,
        output_base: Optional[str] = None,
        retention_days: int = 7,
        voice_map: Optional[Dict[str, str]] = None,
    ):
        self.voice = voice
        self.log = logger or PipelineLogger(component="tts_stub")
        self.output_base = output_base or os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "tts"))
        self.retention_days = retention_days
        self.voice_map = voice_map or {
            "en|news": "en-US-GuyNeural",
            "en|kids": "en-US-JennyNeural",
            "en|youth": "en-US-RogerNeural",
            "hi|news": "hi-IN-AaravNeural",
            "hi|kids": "hi-IN-SwaraNeural",
            "ta|news": "ta-IN-PallaviNeural",
            "bn|news": "bn-IN-TanishaaNeural",
        }
        self.provider = (os.getenv("TTS_PROVIDER") or ("pyttsx3" if pyttsx3 else "stub")).lower()
        self.rate = int(os.getenv("TTS_RATE", "150"))
        self.sample_rate = int(os.getenv("TTS_SAMPLE_RATE", "44100"))
        self.voice_name = os.getenv("TTS_VOICE_NAME")

    def _hash_id(self, title: str, body: str) -> str:
        h = hashlib.sha256((title + "|" + body).encode("utf-8", errors="ignore")).hexdigest()
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
                    elif os.path.isfile(fpath) and fname.lower().endswith(".wav"):
                        try:
                            with wave.open(fpath, "rb") as wf:
                                frames = wf.getnframes()
                                rate = wf.getframerate() or 16000
                                dur = float(frames) / float(rate)
                                if dur <= 1.5:
                                    os.remove(fpath)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def _write_wav(self, path: str, duration_seconds: float = 2.0, sample_rate: int = 16000, text: Optional[str] = None) -> None:
        # 16kHz mono PCM, generate a multi-tone envelope to sound more like voice
        import math
        if text:
            words = [w for w in (text or "").split() if w]
        else:
            words = []
        n_samples = int(duration_seconds * sample_rate)
        base_freq = 220.0
        amplitude = 0.25  # fraction of max to avoid clipping
        with wave.open(path, "w") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)  # 16-bit
            wf.setframerate(sample_rate)
            if words:
                # Generate a short tone per word with a brief pause to avoid "single beep"
                tone_len = max(0.25, min(0.6, 0.35))
                pause_len = 0.08
                for w in words:
                    w_hash = (sum(ord(c) for c in w) % 80) - 40
                    freq = max(140.0, min(360.0, base_freq + float(w_hash)))
                    frames_tone = int(tone_len * sample_rate)
                    for i in range(frames_tone):
                        t = float(i) / sample_rate
                        attack = min(1.0, i / (sample_rate * 0.02))
                        sustain = 0.8
                        release = max(0.2, 1.0 - (i / frames_tone))
                        a = amplitude * attack * sustain * release
                        sample_f = (
                            math.sin(2 * math.pi * freq * t)
                            + 0.3 * math.sin(2 * math.pi * (freq * 2.0) * t)
                        )
                        sample = int(max(-32767, min(32767, a * 32767 * sample_f)))
                        wf.writeframes(struct.pack("<h", sample))
                    # brief pause
                    frames_pause = int(pause_len * sample_rate)
                    silence = struct.pack("<h", 0)
                    for _ in range(frames_pause):
                        wf.writeframes(silence)
            else:
                for i in range(n_samples):
                    t = float(i) / sample_rate
                    # simple ADSR-like envelope
                    attack = min(1.0, i / (sample_rate * 0.02))
                    sustain = 0.8
                    release = max(0.2, 1.0 - (i / n_samples))
                    a = amplitude * attack * sustain * release
                    # sum a few harmonics
                    sample_f = (
                        math.sin(2 * math.pi * base_freq * t)
                        + 0.5 * math.sin(2 * math.pi * (base_freq * 2.0) * t)
                        + 0.25 * math.sin(2 * math.pi * (base_freq * 3.0) * t)
                    )
                    sample = int(max(-32767, min(32767, a * 32767 * sample_f)))
                    wf.writeframes(struct.pack("<h", sample))

    def _duration_for_text(self, text: str) -> float:
        try:
            words = max(1, len((text or "").split()))
            seconds = max(2.0, min(60.0, (words / 2.5) + 0.5))  # ~150 wpm
            return float(seconds)
        except Exception:
            return 6.0

    def synthesize(self, scripts: List[Dict[str, Any]], category: str = "general") -> List[Dict[str, Any]]:
        run = StageLogger(source="pipeline", category=category, meta={"stage": "voice", "voice": self.voice})
        run.start("voice")
        self._cleanup_old()
        outputs: List[Dict[str, Any]] = []
        for s in scripts:
            title = s.get("title") or "Untitled"
            lang = (s.get("lang") or "en").lower()
            narration = (s.get("variants", {}).get("narration") or title)
            tone = (s.get("tone") or "news").lower()
            voice = self.voice_map.get(f"{lang}|{tone}", self.voice)
            # Create deterministic file name
            article_id = self._hash_id(title, narration)
            fname = f"{article_id}_{lang}_{tone}.wav"
            os.makedirs(self.output_base, exist_ok=True)
            audio_path = os.path.join(self.output_base, fname)
            try:
                if self.provider == "pyttsx3" and pyttsx3:
                    try:
                        if pythoncom:
                            pythoncom.CoInitialize()
                        eng = pyttsx3.init()
                        try:
                            voices = eng.getProperty("voices") or []
                            target = None
                            if self.voice_name:
                                for v in voices:
                                    if self.voice_name.lower() in (v.name or "").lower():
                                        target = v.id
                                        break
                            if not target:
                                preferred = ["Zira", "Jenny", "David", "Mark", "Neural"]
                                for name in preferred:
                                    for v in voices:
                                        if name.lower() in (v.name or "").lower():
                                            target = v.id
                                            break
                                    if target:
                                        break
                            if target:
                                eng.setProperty("voice", target)
                        except Exception:
                            pass
                        eng.setProperty("rate", self.rate)
                        eng.save_to_file(narration, audio_path)
                        eng.runAndWait()
                        self.log.info("tts_provider_used", provider="pyttsx3", file=audio_path)
                    except Exception as e:
                        self.log.warning("tts_pyttsx3_failed", error=str(e))
                        self._write_wav(audio_path, duration_seconds=self._duration_for_text(narration), sample_rate=self.sample_rate, text=narration)
                    finally:
                        if pythoncom:
                            pythoncom.CoUninitialize()
                else:
                    if self.provider == "pyttsx3" and not pyttsx3:
                        self.log.warning("tts_provider_unavailable", provider="pyttsx3")
                    self._write_wav(audio_path, duration_seconds=self._duration_for_text(narration), sample_rate=self.sample_rate, text=narration)
            except Exception as e:
                self.log.warning("tts_write_wav_failed", file=audio_path, error=str(e))
            
            # Serve via FastAPI static mount at /data/tts
            audio_url = f"/data/tts/{fname}"

            # Verify file generation
            if os.path.exists(audio_path) and os.path.getsize(audio_path) > 0:
                outputs.append({
                    "id": article_id,
                    "title": title,
                    "lang": lang,
                    "voice": voice,
                    "audio_url": audio_url,
                    "audio_path": audio_path,
                    "status": "success",
                    "metadata": {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "category": category,
                        "format": "wav",
                        "sample_rate": 16000,
                        "channels": 1,
                        "provider": ("pyttsx3" if (self.provider == "pyttsx3" and pyttsx3) else "stub"),
                        "narration_text": narration,
                    },
                })
            else:
                self.log.error("tts_generation_failed_no_file", file=audio_path)
                outputs.append({
                    "title": title,
                    "lang": lang,
                    "voice": voice,
                    "audio_url": None,
                    "audio_path": None,
                    "status": "failed",
                    "error": "generation_failed_no_file",
                    "metadata": {
                        "generated_at": datetime.now(timezone.utc).isoformat(),
                        "category": category,
                        "narration_text": narration,
                    },
                })
        run.complete("voice", meta={"count": len(outputs)})
        run.end_run("completed")
        self.log.info("tts_generated", count=len(outputs))
        return outputs
