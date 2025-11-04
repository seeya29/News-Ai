import json
from typing import Any, Dict, List, Optional

import aiohttp
import asyncio
import requests
from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class DomainAPIFetcher(BaseFetcher):
    async def fetch_async(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        base_url = params.get("base_url")
        endpoint = params.get("endpoint", "")
        query = params.get("query", {})
        headers = params.get("headers", {})
        url = f"{base_url}{endpoint}" if base_url else None
        if not url:
            return [{"title": "DomainAPIFetcher misconfigured", "body": "Missing base_url", "timestamp": None, "raw": {"error": "missing_base_url"}}]
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, params=query, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.json(content_type=None)
                    items = self._normalize_json(data, limit)
                    if logger:
                        logger.log_event("fetch", {"connector": "domain_api", "count": len(items), "url": url})
                    return items
        except Exception as e:
            return [{"title": "DomainAPIFetcher error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        # Concrete wrapper to satisfy BaseFetcher abstract method
        try:
            return asyncio.run(self.fetch_async(limit=limit, logger=logger))
        except RuntimeError:
            # If already in an event loop, run via new loop in a thread
            loop = asyncio.new_event_loop()
            try:
                return loop.run_until_complete(self.fetch_async(limit=limit, logger=logger))
            finally:
                loop.close()

    def _normalize_json(self, data: Any, limit: int) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            seq = data[:limit]
            for obj in seq:
                items.append({"title": str(obj)[:80], "body": json.dumps(obj)[:1000], "timestamp": None, "raw": obj})
        elif isinstance(data, dict):
            # Try to find a list-like key
            for key, val in data.items():
                if isinstance(val, list):
                    for obj in val[:limit]:
                        items.append({"title": str(obj)[:80], "body": json.dumps(obj)[:1000], "timestamp": None, "raw": obj})
                    break
            if not items:
                items.append({"title": "API result", "body": json.dumps(data)[:1000], "timestamp": None, "raw": data})
        else:
            items.append({"title": "API result", "body": str(type(data)), "timestamp": None, "raw": {"type": str(type(data))}})
        return items


class GenericJSONFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        url = params.get("url")
        headers = params.get("headers", {})
        if not url:
            return [{"title": "GenericJSON misconfigured", "body": "Missing url", "timestamp": None, "raw": {"error": "missing_url"}}]
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            items = self._normalize_json(data, limit)
            if logger:
                logger.log_event("fetch", {"connector": "generic_json", "count": len(items), "url": url})
            return items
        except Exception as e:
            return [{"title": "GenericJSON error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]

    def _normalize_json(self, data: Any, limit: int) -> List[Dict[str, Any]]:
        # Same normalization as DomainAPIFetcher
        items: List[Dict[str, Any]] = []
        if isinstance(data, list):
            seq = data[:limit]
            for obj in seq:
                items.append({"title": str(obj)[:80], "body": json.dumps(obj)[:1000], "timestamp": None, "raw": obj})
        elif isinstance(data, dict):
            for key, val in data.items():
                if isinstance(val, list):
                    for obj in val[:limit]:
                        items.append({"title": str(obj)[:80], "body": json.dumps(obj)[:1000], "timestamp": None, "raw": obj})
                    break
            if not items:
                items.append({"title": "API result", "body": json.dumps(data)[:1000], "timestamp": None, "raw": data})
        else:
            items.append({"title": "API result", "body": str(type(data)), "timestamp": None, "raw": {"type": str(type(data))}})
        return items