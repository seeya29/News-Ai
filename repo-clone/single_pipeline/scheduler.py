import os
import time
import json
import threading
from .cli import run_fetch, run_filter, run_scripts, run_voice, run_avatar

def run_once(registry: str = "single", category: str = "general", voice: str = "en-US-Neural-1", style: str = "news-anchor"):
    out = {}
    f = run_fetch(registry=registry, category=category)
    out["fetch"] = f
    try:
        items = f.get("items") or []
        path = f.get("output_file")
        if isinstance(items, list) and len(items) == 0 and isinstance(path, str) and path:
            payload = [
                {
                    "title": "Scheduler seed: tech update",
                    "body": "Generated seed item to keep pipeline active until live feeds populate.",
                    "timestamp": int(time.time()),
                    "category": "tech",
                }
            ]
            try:
                with open(path, "w", encoding="utf-8") as fp:
                    json.dump(payload, fp, ensure_ascii=False, indent=2)
            except Exception:
                pass
    except Exception:
        pass
    out["filter"] = run_filter(registry=registry, category=category)
    out["scripts"] = run_scripts(registry=registry, category=category)
    out["voice"] = run_voice(registry=registry, category=category, voice=voice)
    out["avatar"] = run_avatar(registry=registry, category=category, style=style)
    return out

def start(interval_seconds: int = 300, registry: str = "single", category: str = "general"):
    voice = os.getenv("SCHED_VOICE", "en-US-Neural-1")
    style = os.getenv("SCHED_STYLE", "news-anchor")
    stop_flag = {"v": False}
    def loop():
        while not stop_flag["v"]:
            try:
                run_once(registry, category, voice, style)
            except Exception:
                pass
            time.sleep(max(1, int(interval_seconds)))
    t = threading.Thread(target=loop, daemon=True)
    t.start()
    return {"thread_started": True, "interval_seconds": int(interval_seconds)}
