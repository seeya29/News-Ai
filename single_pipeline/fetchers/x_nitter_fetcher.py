import feedparser
from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class XViaNitterFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        nitter_base = params.get("nitter_base", "https://nitter.net")
        username = params.get("username")
        if not username:
            return [{"title": "X/Nitter misconfigured", "body": "Missing username", "timestamp": None, "raw": {"error": "missing_username"}}]
        url = f"{nitter_base}/{username}/rss"
        feed = feedparser.parse(url)
        items: List[Dict[str, Any]] = []
        for e in feed.entries[:limit]:
            items.append({
                "title": e.get("title", "Tweet"),
                "body": e.get("summary", ""),
                "timestamp": e.get("published"),
                "raw": {"link": e.get("link"), "id": e.get("id")}
            })
        if logger:
            logger.log_event("fetch", {"connector": "x_nitter", "count": len(items), "url": url})
        return items