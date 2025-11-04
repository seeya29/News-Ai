import importlib.util
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class FunctionAdapterFetcher(BaseFetcher):
    """Loads a target Python file and calls its run/process function to fetch items.

    Config example:
    {
        "file": "function_adapter.py",
        "class": "FunctionAdapterFetcher",
        "params": {
            "target_file": "law_agent_wrapper.py",
            "target_function": "process",
            "item_mapping": {
                "title": "title",
                "body": "body",
                "timestamp": "timestamp"
            }
        }
    }
    """

    def __init__(self, cfg: Dict[str, Any]):
        super().__init__(cfg)
        params = cfg.get("params", {})
        self.target_file = Path(params.get("target_file", "")).resolve()
        self.target_function = params.get("target_function", "process")
        self.item_mapping = params.get("item_mapping", {})
        self.items_key = params.get("items_key")

    def _load_function(self):
        if not self.target_file.exists():
            raise FileNotFoundError(f"Target file not found: {self.target_file}")
        spec = importlib.util.spec_from_file_location("target_module", str(self.target_file))
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load spec for {self.target_file}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        func = getattr(module, self.target_function, None)
        if func is None:
            raise ImportError(f"Function {self.target_function} not found in {self.target_file}")
        return func

    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Map fields according to item_mapping; keep raw for reference
        normalized = {
            "title": item.get(self.item_mapping.get("title", "title"), "Untitled"),
            "body": item.get(self.item_mapping.get("body", "body"), ""),
            "timestamp": item.get(self.item_mapping.get("timestamp", "timestamp")),
            "raw": item,
        }
        return normalized

    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        try:
            func = self._load_function()
            output = func({"limit": limit})

            items: List[Dict[str, Any]]
            if isinstance(output, dict):
                if self.items_key and isinstance(output.get(self.items_key), list):
                    items = output[self.items_key][:limit]
                else:
                    items = [output]
            elif isinstance(output, list):
                items = output[:limit]
            else:
                items = [{"title": "FunctionAdapter output error", "body": str(type(output)), "timestamp": None, "raw": {"error": "invalid_output"}}]

            normalized = [self._normalize_item(i) for i in items]
            return normalized
        except Exception as e:
            if logger:
                logger.log_event("fetch", {"adapter": "function", "error": str(e)})
            return [{
                "title": "FunctionAdapter fetch error",
                "body": str(e),
                "timestamp": None,
                "raw": {"error": str(e)}
            }]