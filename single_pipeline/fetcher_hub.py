import os
import json
import asyncio
from typing import Any, Dict, List, Optional

from .logging_utils import PipelineLogger, StageLogger
from .fetchers.rss_fetchers import RSSFetcher
from .fetchers.api_fetchers import DomainAPIFetcher
from .fetchers.live_fetchers import fetch_telegram_channels, fetch_x_handles, fetch_youtube_channels
from .fetchers.stub_fetchers import StubFetcher


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


def _load_sources_config() -> Dict[str, Any]:
    cfg_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "sources.json"))
    if not os.path.exists(cfg_path):
        return {}
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        PipelineLogger(component="fetcher_hub").warning("sources_config_load_failed", file=cfg_path, error=str(e))
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

    def _run_coro_safely(self, coro, label: str) -> int:
        try:
            asyncio.get_running_loop()
            self.log.warning(f"{label}_skipped_async_context")
            return 0
        except RuntimeError:
            return asyncio.run(coro)

    def _write_items(self, name: str, items: List[Dict[str, Any]]) -> str:
        root = _output_root()
        os.makedirs(root, exist_ok=True)
        path = _safe_join(root, f"{_sanitize_identifier(name)}_items.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log.error("write_items_failed", file=path, error=str(e))
        return path

    async def _write_items_async(self, name: str, items: List[Dict[str, Any]]) -> str:
        def _do_write(p: str, payload: List[Dict[str, Any]]):
            with open(p, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        root = _output_root()
        os.makedirs(root, exist_ok=True)
        path = _safe_join(root, f"{_sanitize_identifier(name)}_items.json")
        try:
            await asyncio.to_thread(_do_write, path, items)
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
                ing = self._run_coro_safely(
                    fetch_telegram_channels(
                        tg_channels,
                        int(tg_api_id),
                        tg_api_hash,
                        limit_per_channel=int(tg.get("limit", 20)),
                    ),
                    "telegram_fetch",
                )
                ingested_total += ing
            except Exception as e:
                self.log.warning("telegram_fetch_failed", error=str(e))
        elif tg_channels and (not tg_api_id or not tg_api_hash):
            self.log.warning("telegram_credentials_missing")

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
        elif x_handles and not x_token:
            self.log.warning("x_token_missing")

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
        elif yt_channels and not yt_key:
            self.log.warning("youtube_api_key_missing")

        # Stubs
        stubs_cfg = sources.get("stubs") or []
        stub_fetcher = StubFetcher()
        for entry in stubs_cfg:
            agent_name = entry.get("agent_name")
            name = entry.get("name") or "stub"
            if not agent_name:
                continue
            try:
                stub_items = stub_fetcher.fetch(agent_name)
                for it in stub_items:
                    items.append({
                        "title": it.get("title") or "Untitled",
                        "body": it.get("body") or "",
                        "timestamp": it.get("published_at"),
                        "source": {"name": name, "type": "stub", "agent": agent_name, "url": it.get("link")},
                        "category": it.get("category") or category,
                    })
            except Exception as e:
                self.log.warning("stub_fetch_failed", agent=agent_name, error=str(e))

        out_path = self._write_items(registry_name, items)
        run.update("fetch", progress=100, meta={"items": len(items), "ingested": ingested_total, "file": out_path})
        run.complete("fetch", meta={"items": len(items)})
        run.end_run("completed")
        self.log.info("fetchers_completed", items=len(items), ingested=ingested_total, file=out_path)
        return {"items": items, "output_file": out_path, "ingested": ingested_total}

    async def async_run(self, registry_name: str = "single", category: str = "general") -> Dict[str, Any]:
        cfg = _load_sources_config()
        sources = (cfg.get("registries", {}).get(registry_name) or cfg.get("sources") or {})
        run = StageLogger(source="fetchers", category=category, meta={"registry": registry_name})
        run.start("fetch")
        items: List[Dict[str, Any]] = []

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

        live_cfg = sources.get("live") or {}
        ingested_total = 0
        tg = live_cfg.get("telegram") or {}
        tg_channels = tg.get("channels") or []
        tg_api_id = os.getenv("TELEGRAM_API_ID")
        tg_api_hash = os.getenv("TELEGRAM_API_HASH")
        if tg_channels and tg_api_id and tg_api_hash:
            try:
                ing = await fetch_telegram_channels(tg_channels, int(tg_api_id), tg_api_hash, limit_per_channel=int(tg.get("limit", 20)))
                ingested_total += ing
            except Exception as e:
                self.log.warning("telegram_fetch_failed", error=str(e))
        elif tg_channels and (not tg_api_id or not tg_api_hash):
            self.log.warning("telegram_credentials_missing")

        x = live_cfg.get("x") or {}
        x_handles = x.get("handles") or []
        x_token = os.getenv("TWITTER_BEARER_TOKEN")
        if x_handles and x_token:
            try:
                ing = fetch_x_handles(x_handles, x_token, limit_per_handle=int(x.get("limit", 20)))
                ingested_total += ing
            except Exception as e:
                self.log.warning("x_fetch_failed", error=str(e))
        elif x_handles and not x_token:
            self.log.warning("x_token_missing")

        yt = live_cfg.get("youtube") or {}
        yt_channels = yt.get("channel_ids") or []
        yt_key = os.getenv("YOUTUBE_API_KEY")
        if yt_channels and yt_key:
            try:
                ing = fetch_youtube_channels(yt_channels, yt_key, limit_per_channel=int(yt.get("limit", 20)))
                ingested_total += ing
            except Exception as e:
                self.log.warning("youtube_fetch_failed", error=str(e))
        elif yt_channels and not yt_key:
            self.log.warning("youtube_api_key_missing")

        # Stubs
        stubs_cfg = sources.get("stubs") or []
        stub_fetcher = StubFetcher()
        for entry in stubs_cfg:
            agent_name = entry.get("agent_name")
            name = entry.get("name") or "stub"
            if not agent_name:
                continue
            try:
                stub_items = stub_fetcher.fetch(agent_name)
                for it in stub_items:
                    items.append({
                        "title": it.get("title") or "Untitled",
                        "body": it.get("body") or "",
                        "timestamp": it.get("published_at"),
                        "source": {"name": name, "type": "stub", "agent": agent_name, "url": it.get("link")},
                        "category": it.get("category") or category,
                    })
            except Exception as e:
                self.log.warning("stub_fetch_failed", agent=agent_name, error=str(e))

        out_path = self._write_items(registry_name, items)
        run.update("fetch", progress=100, meta={"items": len(items), "ingested": ingested_total, "file": out_path})
        run.complete("fetch", meta={"items": len(items)})
        run.end_run("completed")
        self.log.info("fetchers_completed", items=len(items), ingested=ingested_total, file=out_path)
        return {"items": items, "output_file": out_path, "ingested": ingested_total}
