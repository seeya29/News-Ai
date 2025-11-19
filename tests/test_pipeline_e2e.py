import os
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
    langs = {f["lang"] for f in filtered}
    # Expect languages detected among en/hi/ta/bn/mixed
    assert any(l in langs for l in ["en", "hi", "ta", "bn"])  # basic sanity

    # Script stage
    sg = ScriptGenAgent()
    scripts = sg.generate(filtered)
    assert len(scripts) == 4
    for s in scripts:
        assert "variants" in s
        assert "narration" in s["variants"]

    # Voice stage
    tts = TTSAgentStub()
    voice_items = tts.synthesize(scripts)
    assert len(voice_items) == 4
    for v in voice_items:
        assert v.get("audio_url")

    # Avatar stage
    avatar = AvatarAgentStub()
    videos = avatar.render(voice_items)
    assert len(videos) == 4
    for vid in videos:
        assert vid.get("video_url")