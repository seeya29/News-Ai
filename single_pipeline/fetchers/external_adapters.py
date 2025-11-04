from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class GurukulAdapterFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        # Stub: integrate with Gurukul pipeline here
        items: List[Dict[str, Any]] = []
        for i in range(min(limit, 5)):
            items.append({
                "title": f"Gurukul Story {i+1}",
                "body": "Sample content from Gurukul adapter.",
                "timestamp": None,
                "raw": {"source": "gurukul_stub"}
            })
        if logger:
            logger.log_event("fetch", {"connector": "gurukul_stub", "count": len(items)})
        return items


class StockAgentAdapterFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for i in range(min(limit, 5)):
            items.append({
                "title": f"Stock Update {i+1}",
                "body": "Sample stock highlight content.",
                "timestamp": None,
                "raw": {"source": "stock_agent_stub"}
            })
        if logger:
            logger.log_event("fetch", {"connector": "stock_agent_stub", "count": len(items)})
        return items


class WellnessBotAdapterFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for i in range(min(limit, 5)):
            items.append({
                "title": f"Wellness Tip {i+1}",
                "body": "Daily wellness guidance sample.",
                "timestamp": None,
                "raw": {"source": "wellness_bot_stub"}
            })
        if logger:
            logger.log_event("fetch", {"connector": "wellness_bot_stub", "count": len(items)})
        return items


class UsedCarAdapterFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        for i in range(min(limit, 5)):
            items.append({
                "title": f"Used Car Listing {i+1}",
                "body": "Sample listing details.",
                "timestamp": None,
                "raw": {"source": "used_car_stub"}
            })
        if logger:
            logger.log_event("fetch", {"connector": "used_car_stub", "count": len(items)})
        return items