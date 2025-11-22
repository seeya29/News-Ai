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
                except Exception:
                    pass
        except Exception:
            pass

    def _write_wav(self, path: str, duration_seconds: float = 2.0, sample_rate: int = 16000) -> None:
        # 16kHz mono PCM, write a simple 440 Hz sine tone so it's audible
        import math
        n_samples = int(duration_seconds * sample_rate)
        freq = 440.0
        amplitude = 0.25  # fraction of max to avoid clipping
        with wave.open(path, "w") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)  # 16-bit
            wf.setframerate(sample_rate)
            for i in range(n_samples):
                t = float(i) / sample_rate
                a = amplitude if (i // (sample_rate // 2)) % 2 == 0 else amplitude * 0.6
                sample = int(max(-32767, min(32767, a * 32767 * math.sin(2 * math.pi * freq * t))))
                wf.writeframes(struct.pack("<h", sample))

    def _duration_for_text(self, text: str) -> float:
        try:
            n = max(2, min(30, int(len(text) / 20)))
            return float(n)
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
                if pyttsx3:
                    try:
                        eng = pyttsx3.init()
                        eng.setProperty("rate", 170)
                        eng.save_to_file(narration, audio_path)
                        eng.runAndWait()
                    except Exception:
                        self._write_wav(audio_path, duration_seconds=self._duration_for_text(narration), sample_rate=16000)
                else:
                    self._write_wav(audio_path, duration_seconds=self._duration_for_text(narration), sample_rate=16000)
            except Exception as e:
                self.log.warning("tts_write_wav_failed", file=audio_path, error=str(e))
            # Serve via FastAPI static mount at /data/tts
            audio_url = f"/data/tts/{fname}"
            outputs.append({
                "title": title,
                "lang": lang,
                "voice": voice,
                "audio_url": audio_url,
                "audio_path": audio_path,
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "category": category,
                    "format": "wav",
                    "sample_rate": 16000,
                    "channels": 1,
                },
            })
        run.complete("voice", meta={"count": len(outputs)})
        run.end_run("completed")
        self.log.info("tts_generated", count=len(outputs))
        return outputs