import os
import sys
import json
import argparse
from typing import List, Dict, Any

# Ensure project root is on sys.path so sibling packages import correctly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

from single_pipeline.agents.tts_agent_stub import TTSAgentStub
from single_pipeline.agents.avatar_agent_stub import AvatarAgentStub


def ensure_ffmpeg_on_path() -> None:
    try:
        pkg_root = os.path.join(
            os.environ.get("LOCALAPPDATA", ""),
            "Microsoft",
            "WinGet",
            "Packages",
            "Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe",
            "ffmpeg-8.0.1-full_build",
            "bin",
        )
        if os.path.isdir(pkg_root):
            os.environ["PATH"] = pkg_root + os.pathsep + os.environ.get("PATH", "")
    except Exception:
        pass


def synthesize_text(narration_text: str, title: str = "Seeya Intro") -> List[Dict[str, Any]]:
    agent = TTSAgentStub()
    scripts = [{
        "title": title,
        "lang": "en",
        "tone": "news",
        "variants": {"narration": narration_text},
    }]
    return agent.synthesize(scripts, category="general")


def render_avatar(voice_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ensure_ffmpeg_on_path()
    agent = AvatarAgentStub(style="news-anchor")
    return agent.render(voice_items, category="general")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--text", required=True)
    parser.add_argument("--title", default="Seeya Intro")
    parser.add_argument("--base", default=os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000"))
    args = parser.parse_args()

    voice_items = synthesize_text(args.text, title=args.title)
    avatar_items = render_avatar(voice_items)

    audio_route = str(voice_items[0].get("audio_url"))
    video_route = str(avatar_items[0].get("video_url"))
    audio_path = str(voice_items[0].get("audio_path"))
    meta_path = str(avatar_items[0].get("metadata_path"))

    base = args.base.rstrip("/")
    out = {
        "audio_url": base + audio_route,
        "video_url": base + video_route,
        "audio_path": audio_path,
        "metadata_path": meta_path,
        "status": avatar_items[0].get("metadata", {}).get("category", "general"),
    }
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
