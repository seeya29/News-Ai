import os
import json
import asyncio
from typing import Any, Dict, List, Optional

from .logging_utils import PipelineLogger, StageLogger
from .fetchers.rss_fetchers import RSSFetcher
from .fetchers.api_fetchers import DomainAPIFetcher
from .fetchers.live_fetchers import fetch_telegram_channels, fetch_x_handles, fetch_youtube_channels


def _output_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "output"))


def _load_sources_config() -> Dict[str, Any]:
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "sources.json"))
    if not os.path.exists(cfg_path):
        return {}
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


class FetcherHub:
    """Plug-and-play registry to orchestrate fetchers based on sources.json.

    Supports:
    - RSS feeds
    - Domain API URLs
    - Live sources: Telegram, X, YouTube
    Writes consolidated items to single_pipeline/output/*_items.json for demo and server consumption.
    """

    def __init__(self, logger: Optional[PipelineLogger] = None):
        self.log = logger or PipelineLogger(component="fetcher_hub")

    def _write_items(self, name: str, items: List[Dict[str, Any]]) -> str:
        root = _output_root()
        os.makedirs(root, exist_ok=True)
        path = os.path.join(root, f"{name}_items.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log.error("write_items_failed", file=path, error=str(e))
        return path

    def run(self, registry_name: str = "single", category: str = "general") -> Dict[str, Any]:
        cfg = _load_sources_config()
        sources = (cfg.get("registries", {}).get(registry_name) or cfg.get("sources") or {})
        run = StageLogger(source="fetchers", category=category, meta={"registry": registry_name})
        run.start("fetch")
        items: List[Dict[str, Any]] = []

        # RSS feeds
        rss_cfg = sources.get("rss") or []
        rss = RSSFetcher()
        for entry in rss_cfg:
            url = entry.get("url")
            name = entry.get("name") or "rss"
            if not url:
                continue
            res = rss.fetch(url)
            if res.get("result") == "ok":
                for it in res.get("items", []) or []:
                    items.append({
                        "title": it.get("title") or "Untitled",
                        "body": it.get("summary") or "",
                        "timestamp": it.get("published"),
                        "source": {"name": name, "type": "rss", "url": it.get("link")},
                        "category": category,
                    })
            else:
                self.log.warning("rss_entry_failed", url=url, error=res.get("error"))

        # Domain API endpoints
        api_cfg = sources.get("api") or []
        api = DomainAPIFetcher()
        for entry in api_cfg:
            url = entry.get("url")
            name = entry.get("name") or "api"
            params = entry.get("params") or None
            headers = entry.get("headers") or None
            if not url:
                continue
            res = api.fetch(url, params=params, headers=headers)
            if res.get("result") == "ok":
                data = res.get("data")
                # Assume either list of items or dict with items
                if isinstance(data, list):
                    for it in data:
                        title = (it.get("title") if isinstance(it, dict) else str(it))
                        items.append({
                            "title": title or "Untitled",
                            "body": (it.get("summary") or it.get("body") or "") if isinstance(it, dict) else "",
                            "timestamp": (it.get("published") or it.get("timestamp") if isinstance(it, dict) else None),
                            "source": {"name": name, "type": "api", "url": url},
                            "category": category,
                        })
                elif isinstance(data, dict):
                    for it in (data.get("items") or []):
                        items.append({
                            "title": it.get("title") or "Untitled",
                            "body": it.get("summary") or it.get("body") or "",
                            "timestamp": it.get("published") or it.get("timestamp"),
                            "source": {"name": name, "type": "api", "url": url},
                            "category": category,
                        })
            else:
                self.log.warning("api_entry_failed", url=url, error=res.get("error"))

        # Live sources (optional)
        live_cfg = sources.get("live") or {}
        ingested_total = 0
        # Telegram
        tg = live_cfg.get("telegram") or {}
        tg_channels = tg.get("channels") or []
        tg_api_id = os.getenv("TELEGRAM_API_ID")
        tg_api_hash = os.getenv("TELEGRAM_API_HASH")
        if tg_channels and tg_api_id and tg_api_hash:
            try:
                ing = asyncio.run(fetch_telegram_channels(tg_channels, int(tg_api_id), tg_api_hash, limit_per_channel=int(tg.get("limit", 20))))
                ingested_total += ing
            except Exception as e:
                self.log.warning("telegram_fetch_failed", error=str(e))

        # X (Twitter)
        x = live_cfg.get("x") or {}
        x_handles = x.get("handles") or []
        x_token = os.getenv("TWITTER_BEARER_TOKEN")
        if x_handles and x_token:
            try:
                ing = fetch_x_handles(x_handles, x_token, limit_per_handle=int(x.get("limit", 20)))
                ingested_total += ing
            except Exception as e:
                self.log.warning("x_fetch_failed", error=str(e))

        # YouTube
        yt = live_cfg.get("youtube") or {}
        yt_channels = yt.get("channel_ids") or []
        yt_key = os.getenv("YOUTUBE_API_KEY")
        if yt_channels and yt_key:
            try:
                ing = fetch_youtube_channels(yt_channels, yt_key, limit_per_channel=int(yt.get("limit", 20)))
                ingested_total += ing
            except Exception as e:
                self.log.warning("youtube_fetch_failed", error=str(e))

        out_path = self._write_items(registry_name, items)
        run.update("fetch", progress=100, meta={"items": len(items), "ingested": ingested_total, "file": out_path})
        run.complete("fetch", meta={"items": len(items)})
        run.end_run("completed")
        self.log.info("fetchers_completed", items=len(items), ingested=ingested_total, file=out_path)
        return {"items": items, "output_file": out_path, "ingested": ingested_total}