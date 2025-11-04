import asyncio
from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger

import aiohttp


class HTTPJSONFetcher(BaseFetcher):
    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        params = cfg.get("params", {})
        self.url = params.get("url")
        self.items_key = params.get("items_key")  # dot path
        self.title_key = params.get("title_key", "title")
        self.body_key = params.get("body_key", "body")
        self.timestamp_key = params.get("timestamp_key", "timestamp")
        self.headers = params.get("headers", {})
        self.query = params.get("query", {})

    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        if self.items_key and isinstance(data, dict):
            cur = data
            for part in self.items_key.split('.'):
                if isinstance(cur, dict):
                    cur = cur.get(part)
                else:
                    cur = None
                    break
            if isinstance(cur, list):
                return cur
        if isinstance(data, list):
            return data
        return [data]

    async def fetch_async(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        if not self.url:
            return [{"title": "HTTPJSONFetcher error", "body": "No URL provided", "timestamp": None, "raw": {"error": "missing_url"}}]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, headers=self.headers, params=self.query, timeout=15) as resp:
                    data = await resp.json(content_type=None)
            items_raw = self._extract_items(data)
            items: List[Dict[str, Any]] = []
            for obj in items_raw[:limit]:
                if isinstance(obj, dict):
                    items.append({
                        "title": obj.get(self.title_key, "Untitled"),
                        "body": obj.get(self.body_key, ""),
                        "timestamp": obj.get(self.timestamp_key),
                        "raw": obj
                    })
                else:
                    items.append({"title": "HTTPJSON item", "body": str(obj), "timestamp": None, "raw": {"value": obj}})
            if logger:
                logger.log_event("fetch", {"connector": "http_json", "count": len(items), "url": self.url})
            return items
        except Exception as e:
            return [{"title": "HTTPJSONFetcher error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        return asyncio.run(self.fetch_async(limit, logger))