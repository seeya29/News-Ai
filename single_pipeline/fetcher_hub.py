import importlib.util
import json
import asyncio
from pathlib import Path
from typing import Any, Dict, List, Optional

from logging_utils import PipelineLogger
from base_fetcher import BaseFetcher

try:
    import yaml
except Exception:
    yaml = None


class FetcherHub:
    def __init__(self, registry_path: Path):
        self.registry_path = registry_path
        if not self.registry_path.exists():
            raise FileNotFoundError(f"Registry file not found: {registry_path}")
        self.registry = self._load_registry(self.registry_path)

    def _load_registry(self, path: Path) -> Dict[str, Any]:
        suffix = path.suffix.lower()
        text = path.read_text(encoding="utf-8")
        if suffix in {".yaml", ".yml"}:
            if yaml is None:
                raise ImportError("PyYAML is required to load YAML registries")
            return yaml.safe_load(text)
        return json.loads(text)

    def _load_class_from_file(self, file_path: Path, class_name: str):
        spec = importlib.util.spec_from_file_location(class_name, str(file_path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load spec for {file_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ImportError(f"Class {class_name} not found in {file_path}")
        return cls

    def _load_class_from_module(self, module_path: str, class_name: str):
        import importlib
        module = importlib.import_module(module_path)
        cls = getattr(module, class_name, None)
        if cls is None:
            raise ImportError(f"Class {class_name} not found in module {module_path}")
        return cls

    def _init_fetcher(self, cfg: Dict[str, Any]) -> BaseFetcher:
        class_name = cfg.get("class") or cfg.get("adapter")
        if not class_name:
            raise ValueError("Fetcher config must include 'class' or 'adapter'")
        if cfg.get("module"):
            cls = self._load_class_from_module(cfg["module"], class_name)
        elif cfg.get("file"):
            raw_path = Path(cfg["file"])
            file_path = raw_path if raw_path.is_absolute() else (self.registry_path.parent / raw_path)
            file_path = file_path.resolve()
            cls = self._load_class_from_file(file_path, class_name)
        else:
            raise ValueError("Fetcher config must include 'module' or 'file'")
        return cls(cfg)

    async def _fetch_connector_async(self, key: str, cfg: Dict[str, Any], limit: int, logger: Optional[PipelineLogger]) -> List[Dict[str, Any]]:
        if not cfg.get("enabled", True):
            return []
        try:
            fetcher = self._init_fetcher(cfg)
            if hasattr(fetcher, "fetch_async"):
                return await fetcher.fetch_async(limit=limit, logger=logger)
            # Fallback to sync in a thread
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, fetcher.fetch, limit, logger)
        except Exception as e:
            if logger:
                logger.log_event("fetch", {"connector": key, "error": str(e)})
            return [{
                "connector": key,
                "title": f"Fetcher error in {key}",
                "body": str(e),
                "timestamp": None,
                "raw": {"error": str(e)}
            }]

    async def fetch_async(self, sources: Optional[List[str]], limit: int, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        connectors: Dict[str, Dict[str, Any]] = self.registry.get("connectors", {})
        tasks = []
        for key, cfg in connectors.items():
            if sources and key not in sources:
                continue
            tasks.append(self._fetch_connector_async(key, cfg, limit, logger))
        results = await asyncio.gather(*tasks, return_exceptions=False)
        for res in results:
            items.extend(res)
        return items

    def fetch(self, sources: Optional[List[str]], limit: int, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        return asyncio.run(self.fetch_async(sources, limit, logger))