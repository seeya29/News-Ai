import os
import json
from typing import Any, Dict, List, Optional

from .fetcher_hub import FetcherHub
from .agents.filter_agent import FilterAgent
from .agents.script_gen_agent import ScriptGenAgent
from .agents.tts_agent_stub import TTSAgentStub
from .agents.avatar_agent_stub import AvatarAgentStub
from .logging_utils import StageLogger, PipelineLogger
from .bucket_orchestrator import BucketOrchestrator


def _output_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "output"))

def _sanitize_identifier(s: str) -> str:
    s = (s or "").strip()
    import re as _re
    cleaned = _re.sub(r"[^a-zA-Z0-9_-]", "", s)
    return cleaned or "default"

def _safe_join(root: str, relative: str) -> str:
    root_abs = os.path.abspath(root)
    cand = os.path.abspath(os.path.join(root_abs, relative))
    if cand == root_abs or cand.startswith(root_abs + os.sep):
        return cand
    return root_abs


def _read_items(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return data.get("items") or []
    except Exception:
        return []


def _write_json(name: str, suffix: str, payload: Any) -> str:
    root = _output_root()
    os.makedirs(root, exist_ok=True)
    path = _safe_join(root, f"{_sanitize_identifier(name)}_{suffix}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def run_fetch(registry: str = "single", category: str = "general") -> Dict[str, Any]:
    hub = FetcherHub()
    res = hub.run(registry_name=registry, category=category)
    return res

async def run_fetch_async(registry: str = "single", category: str = "general") -> Dict[str, Any]:
    hub = FetcherHub()
    res = await hub.async_run(registry_name=registry, category=category)
    return res


def run_filter(registry: str = "single", category: str = "general") -> Dict[str, Any]:
    log = PipelineLogger(component="cli_filter")
    run = StageLogger(source="pipeline", category=category, meta={"registry": registry})
    run.start("filter")

    items_path = _safe_join(_output_root(), f"{_sanitize_identifier(registry)}_items.json")
    items = _read_items(items_path)
    agent = FilterAgent()
    filtered = agent.filter_items(items, logger=log)
    out_path = _write_json(registry, "filtered", filtered)
    run.complete("filter", meta={"count": len(filtered), "file": out_path})
    run.end_run("completed")
    return {"count": len(filtered), "output_file": out_path}


def run_scripts(registry: str = "single", category: str = "general") -> Dict[str, Any]:
    log = PipelineLogger(component="cli_scripts")
    run = StageLogger(source="pipeline", category=category, meta={"registry": registry})
    run.start("summarize")
    filtered_path = _safe_join(_output_root(), f"{_sanitize_identifier(registry)}_filtered.json")
    items = _read_items(filtered_path)
    agent = ScriptGenAgent(logger=log)
    scripts = agent.generate(items, category=category)
    out_path = _write_json(registry, "scripts", scripts)
    run.complete("summarize", meta={"count": len(scripts), "file": out_path})
    run.end_run("completed")
    return {"count": len(scripts), "output_file": out_path}


def run_voice(
    registry: str = "single",
    category: str = "general",
    voice: str = "en-US-Neural-1",
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    log = PipelineLogger(component="cli_voice")
    run = StageLogger(source="pipeline", category=category, meta={"registry": registry})
    run.start("voice")
    scripts_path = _safe_join(_output_root(), f"{_sanitize_identifier(registry)}_scripts.json")
    scripts = _read_items(scripts_path)
    if isinstance(limit, int) and limit > 0:
        scripts = scripts[:limit]
    agent = TTSAgentStub(voice=voice, logger=log)
    voice_items = agent.synthesize(scripts, category=category)
    out_path = _write_json(_sanitize_identifier(registry), "voice", voice_items)
    run.complete("voice", meta={"count": len(voice_items), "file": out_path})
    run.end_run("completed")
    return {"count": len(voice_items), "output_file": out_path}


def run_avatar(
    registry: str = "single",
    category: str = "general",
    style: str = "news-anchor",
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    log = PipelineLogger(component="cli_avatar")
    run = StageLogger(source="pipeline", category=category, meta={"registry": registry})
    run.start("avatar")
    voice_path = _safe_join(_output_root(), f"{_sanitize_identifier(registry)}_voice.json")
    voice_items = _read_items(voice_path)
    if isinstance(limit, int) and limit > 0:
        voice_items = voice_items[:limit]
    agent = AvatarAgentStub(style=style, logger=log)
    videos = agent.render(voice_items, category=category)
    out_path = _write_json(registry, "avatar", videos)
    run.complete("avatar", meta={"count": len(videos), "file": out_path})
    run.end_run("completed")
    return {"count": len(videos), "output_file": out_path}


if __name__ == "__main__":
    import argparse
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="News-Ai single pipeline CLI")
    sub = parser.add_subparsers(dest="cmd")

    fch = sub.add_parser("fetch", help="Run fetchers from registry")
    fch.add_argument("--registry", default="single")
    fch.add_argument("--category", default="general")

    flt = sub.add_parser("filter", help="Run FilterAgent over items")
    flt.add_argument("--registry", default="single")
    flt.add_argument("--category", default="general")

    scr = sub.add_parser("scripts", help="Generate scripts from filtered items")
    scr.add_argument("--registry", default="single")
    scr.add_argument("--category", default="general")

    vce = sub.add_parser("voice", help="Generate TTS voice from scripts")
    vce.add_argument("--registry", default="single")
    vce.add_argument("--category", default="general")
    vce.add_argument("--voice", default="en-US-Neural-1")
    vce.add_argument("--limit", type=int, default=None)

    av = sub.add_parser("avatar", help="Render avatar videos from voice")
    av.add_argument("--registry", default="single")
    av.add_argument("--category", default="general")
    av.add_argument("--style", default="news-anchor")
    av.add_argument("--limit", type=int, default=None)

    bkt = sub.add_parser("buckets", help="Run bucketed orchestration across stages")
    bkt.add_argument("--registry", default="single")
    bkt.add_argument("--category", default="general")

    args = parser.parse_args()
    if args.cmd == "fetch":
        out = run_fetch(registry=args.registry, category=args.category)
        print(json.dumps(out, ensure_ascii=False))
    elif args.cmd == "filter":
        out = run_filter(registry=args.registry, category=args.category)
        print(json.dumps(out, ensure_ascii=False))
    elif args.cmd == "scripts":
        out = run_scripts(registry=args.registry, category=args.category)
        print(json.dumps(out, ensure_ascii=False))
    elif args.cmd == "voice":
        out = run_voice(registry=args.registry, category=args.category, voice=args.voice, limit=args.limit)
        print(json.dumps(out, ensure_ascii=False))
    elif args.cmd == "avatar":
        out = run_avatar(registry=args.registry, category=args.category, style=args.style, limit=args.limit)
        print(json.dumps(out, ensure_ascii=False))
    elif args.cmd == "buckets":
        orch = BucketOrchestrator(registry=args.registry, category=args.category)
        out = orch.run()
        print(json.dumps(out, ensure_ascii=False))
    else:
        parser.print_help()
