import os
import sys
import json

# Ensure repo root is on import path for package imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from single_pipeline.cli import run_filter, run_scripts, run_voice, run_avatar


def _output_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output"))


def _write_items(items):
    root = _output_root()
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, "single_items.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)
    return path


def _read_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def test_pipeline_chain_multilang(tmp_path):
    # Prepare mixed-language sample items
    items = [
        {"title": "AI breakthrough in chips", "body": "New AI hardware announced by startups.", "timestamp": 1700000000, "category": "tech"},
        {"title": "भारत में मौसम", "body": "आज दिल्ली में बारिश हो सकती है।", "timestamp": 1700000010, "category": "general"},
        {"title": "தமிழ் செய்திகள்", "body": "சென்னையில் புதிய தொழில்நுட்ப மாநாடு.", "timestamp": 1700000020, "category": "tech"},
        {"title": "বাংলা খবর", "body": "কলকাতায় নতুন স্টার্টআপ ফান্ডিং", "timestamp": 1700000030, "category": "finance"},
        {"title": "اردو خبریں", "body": "کراچی میں کھیلوں کا بڑا ایونٹ", "timestamp": 1700000040, "category": "general"},
        {"title": "Kids science fair", "body": "Students present projects on robotics.", "timestamp": 1700000050, "category": "science"},
    ]

    # Write items to output
    items_path = _write_items(items)
    assert os.path.exists(items_path)

    # Enable local Uniguru provider for tone/audience tagging
    os.environ["UNIGURU_PROVIDER"] = "local"

    # Run stages
    fl = run_filter(registry="single", category="general")
    assert fl["count"] >= 6
    filtered_path = os.path.join(_output_root(), "single_filtered.json")
    assert os.path.exists(filtered_path)
    filtered = _read_json(filtered_path)
    assert isinstance(filtered, list)
    # Check language detection and Uniguru tags present
    langs = set([it.get("lang") for it in filtered])
    assert any(l in ("hi", "ta", "bn", "ur") for l in langs)
    assert any(it.get("audience") in ("news", "kids", "youth") for it in filtered)

    sc = run_scripts(registry="single", category="general")
    assert sc["count"] == len(filtered)
    scripts_path = os.path.join(_output_root(), "single_scripts.json")
    assert os.path.exists(scripts_path)
    scripts = _read_json(scripts_path)
    assert isinstance(scripts, list)
    assert scripts[0].get("variants") is not None

    vc = run_voice(registry="single", category="general")
    assert vc["count"] == len(scripts)
    voice_path = os.path.join(_output_root(), "single_voice.json")
    assert os.path.exists(voice_path)
    voice_items = _read_json(voice_path)
    assert isinstance(voice_items, list)
    assert voice_items[0].get("audio_url") is not None

    av = run_avatar(registry="single", category="general")
    assert av["count"] == len(voice_items)
    avatar_path = os.path.join(_output_root(), "single_avatar.json")
    assert os.path.exists(avatar_path)
    videos = _read_json(avatar_path)
    assert isinstance(videos, list)
    assert videos[0].get("video_url") is not None