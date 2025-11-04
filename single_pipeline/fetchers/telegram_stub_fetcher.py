import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class TelegramStubFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        messages_file = params.get("messages_file")
        if not messages_file:
            return [{"title": "TelegramStub misconfigured", "body": "Missing messages_file", "timestamp": None, "raw": {"error": "missing_messages_file"}}]
        path = Path(messages_file)
        if not path.exists():
            return [{"title": "TelegramStub missing file", "body": str(path), "timestamp": None, "raw": {"error": "file_not_found"}}]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            messages = data if isinstance(data, list) else data.get("messages", [])
            items: List[Dict[str, Any]] = []
            for m in messages[:limit]:
                items.append({
                    "title": m.get("title", m.get("sender", "TG Message")),
                    "body": m.get("text", ""),
                    "timestamp": m.get("date"),
                    "raw": m
                })
            if logger:
                logger.log_event("fetch", {"connector": "telegram_stub", "count": len(items), "file": str(path)})
            return items
        except Exception as e:
            return [{"title": "TelegramStub error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]