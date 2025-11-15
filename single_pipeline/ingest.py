import asyncio
import json
import os
from typing import Dict

from server.db import init_db
from single_pipeline.logging_utils import StageLogger
from single_pipeline.fetchers.live_fetchers import (
    fetch_telegram_channels,
    fetch_x_handles,
    fetch_youtube_channels,
)


def _load_env() -> Dict[str, str]:
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass
    return {
        "TELEGRAM_API_ID": os.getenv("TELEGRAM_API_ID"),
        "TELEGRAM_API_HASH": os.getenv("TELEGRAM_API_HASH"),
        "TWITTER_BEARER_TOKEN": os.getenv("TWITTER_BEARER_TOKEN"),
        "YOUTUBE_API_KEY": os.getenv("YOUTUBE_API_KEY"),
    }


def _load_sources() -> Dict:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "sources.json"))
    with open(root, "r", encoding="utf-8") as f:
        return json.load(f)


async def _loop_telegram(sources: Dict, env: Dict):
    cadence = int(sources.get("cadence_seconds", {}).get("telegram", 120))
    channels = sources.get("telegram_channels", [])
    api_id = env.get("TELEGRAM_API_ID")
    api_hash = env.get("TELEGRAM_API_HASH")
    while True:
        try:
            run = StageLogger(source="telegram", category="tech", meta={"channels": channels})
            run.start("fetch", meta={"limit_per_channel": sources.get("limits", {}).get("telegram", 20)})
            n = await fetch_telegram_channels(channels, int(api_id) if api_id else None, api_hash)
            run.update("fetch", progress=100, meta={"items": n})
            run.complete("fetch", meta={"items": n})
            run.end_run("completed")
            print(f"[ingest] telegram fetched {n} items")
        except Exception as e:
            try:
                run.error("fetch", error_code="telegram_fetch_error", error_message=str(e))
                run.end_run("failed")
            except Exception:
                pass
            print(f"[ingest] telegram loop error: {e}")
        await asyncio.sleep(cadence)


async def _loop_x_handles(sources: Dict, env: Dict):
    cadence = int(sources.get("cadence_seconds", {}).get("x_handles", 180))
    handles = sources.get("x_handles", [])
    token = env.get("TWITTER_BEARER_TOKEN")
    while True:
        try:
            run = StageLogger(source="x", category="tech", meta={"handles": handles})
            run.start("fetch", meta={"limit_per_handle": sources.get("limits", {}).get("x_handles", 20)})
            n = fetch_x_handles(handles, token)
            run.update("fetch", progress=100, meta={"items": n})
            run.complete("fetch", meta={"items": n})
            run.end_run("completed")
            print(f"[ingest] x(handles) fetched {n} items")
        except Exception as e:
            try:
                run.error("fetch", error_code="x_fetch_error", error_message=str(e))
                run.end_run("failed")
            except Exception:
                pass
            print(f"[ingest] x(handles) loop error: {e}")
        await asyncio.sleep(cadence)


async def _loop_youtube(sources: Dict, env: Dict):
    cadence = int(sources.get("cadence_seconds", {}).get("youtube", 900))
    channels = sources.get("youtube_channels", [])
    api_key = env.get("YOUTUBE_API_KEY")
    while True:
        try:
            run = StageLogger(source="youtube", category="tech", meta={"channels": channels})
            run.start("fetch", meta={"limit_per_channel": sources.get("limits", {}).get("youtube", 20)})
            n = fetch_youtube_channels(channels, api_key)
            run.update("fetch", progress=100, meta={"items": n})
            run.complete("fetch", meta={"items": n})
            run.end_run("completed")
            print(f"[ingest] youtube fetched {n} items")
        except Exception as e:
            try:
                run.error("fetch", error_code="youtube_fetch_error", error_message=str(e))
                run.end_run("failed")
            except Exception:
                pass
            print(f"[ingest] youtube loop error: {e}")
        await asyncio.sleep(cadence)


async def main():
    init_db()
    env = _load_env()
    sources = _load_sources()

    tasks = [
        asyncio.create_task(_loop_telegram(sources, env)),
        asyncio.create_task(_loop_x_handles(sources, env)),
        asyncio.create_task(_loop_youtube(sources, env)),
    ]
    print("[ingest] started live ingestion loops")
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[ingest] stopped")