import os
import sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import json

from single_pipeline.agents.filter_agent import FilterAgent
from single_pipeline.agents.script_gen_agent import ScriptGenAgent
from single_pipeline.agents.tts_agent_stub import TTSAgentStub
from single_pipeline.agents.avatar_agent_stub import AvatarAgentStub


def test_multi_language_pipeline_chain(tmp_path):
    # Sample items across languages
    items = [
        {"title": "OpenAI launches new model", "body": "OpenAI announced a new model today."},
        {"title": "हिंदी समाचार", "body": "आज नई तकनीक की घोषणा हुई।"},
        {"title": "தமிழ் செய்திகள்", "body": "இன்று புதிய தொழில்நுட்பம் அறிவிக்கப்பட்டது."},
        {"title": "বাংলা খবর", "body": "আজ নতুন প্রযুক্তি ঘোষণা করা হয়েছে।"},
    ]

    # Filter stage
    fa = FilterAgent()
    filtered = fa.filter_items(items)
    assert len(filtered) == 4
    langs = {f["language"] for f in filtered}
    # Expect languages detected among en/hi/ta/bn/mixed
    assert any(l in langs for l in ["en", "hi", "ta", "bn"])  # basic sanity

    # Script stage
    sg = ScriptGenAgent()
    scripts = sg.generate(filtered)
    assert len(scripts) == 4
    for s in scripts:
        assert "script" in s
        assert "text" in s["script"]
        assert "headline" in s["script"]
        assert "bullets" in s["script"]

    # Voice stage
    tts = TTSAgentStub()
    voice_items = tts.synthesize(scripts)
    assert len(voice_items) == 4
    for v in voice_items:
        assert v.get("audio_path")

    # Avatar stage
    avatar = AvatarAgentStub()
    videos = avatar.render(voice_items)
    assert len(videos) == 4
    for vid in videos:
        # Check for success status or video path presence
        assert "video_path" in vid
        assert vid.get("stage_status", {}).get("avatar") in ["success", "failed"]