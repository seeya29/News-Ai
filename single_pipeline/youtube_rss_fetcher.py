import asyncio
from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger

try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None


class YouTubeRSSFetcher(BaseFetcher):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        params = cfg.get("params", {})
        channel_id = params.get("channel_id")
        self.url = params.get("url") or (f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}" if channel_id else None)

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        if feedparser is None:
            return [{
                "title": "YouTubeRSSFetcher error",
                "body": "feedparser not installed",
                "timestamp": None,
                "raw": {"error": "missing_feedparser"}
            }]
        if not self.url:
            return [{"title": "YouTubeRSSFetcher error", "body": "No URL/channel_id provided", "timestamp": None, "raw": {}}]
        parsed = feedparser.parse(self.url)
        items: List[Dict[str, Any]] = []
        for entry in parsed.entries[:limit]:
            items.append({
                "title": getattr(entry, "title", "Untitled"),
                "body": getattr(entry, "summary", ""),
                "timestamp": getattr(entry, "published", None),
                "raw": {"link": getattr(entry, "link", None)}
            })
        return items

    async def fetch_async(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.fetch, limit, logger)