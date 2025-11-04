import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class HackerNewsFetcher(BaseFetcher):
    API_TOP = "https://hacker-news.firebaseio.com/v0/topstories.json"
    API_ITEM = "https://hacker-news.firebaseio.com/v0/item/{id}.json"

    async def fetch_async(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.API_TOP, timeout=15) as resp:
                    ids = await resp.json()
                ids = ids[:limit]
                items: List[Dict[str, Any]] = []
                for i in ids:
                    async with session.get(self.API_ITEM.format(id=i), timeout=15) as r2:
                        data = await r2.json()
                    items.append({
                        "title": data.get("title", "Untitled"),
                        "body": data.get("url", ""),
                        "timestamp": data.get("time"),
                        "raw": data
                    })
            if logger:
                logger.log_event("fetch", {"connector": "hackernews", "count": len(items)})
            return items
        except Exception as e:
            return [{"title": "HackerNews error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        return asyncio.run(self.fetch_async(limit, logger))