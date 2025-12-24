import os
import json
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml  # type: ignore
except Exception:
    yaml = None

from .logging_utils import PipelineLogger


DEFAULT_REGISTRY_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "feed_registry.yaml"))
SOURCES_JSON_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "sources.json"))


_log = PipelineLogger(component="registry")


def _ensure_dirs() -> None:
    os.makedirs(os.path.dirname(SOURCES_JSON_PATH), exist_ok=True)


def load_registry(path: Optional[str] = None) -> Dict[str, Any]:
    """Load the YAML registry from disk.

    Returns a dict with key "feeds": [ { id, type, cadence_seconds, ... } ]
    """
    reg_path = os.path.abspath(path or DEFAULT_REGISTRY_PATH)
    if yaml is None:
        raise RuntimeError("pyyaml not installed; please install 'PyYAML' to use YAML registry")
    if not os.path.exists(reg_path):
        return {"feeds": []}
    try:
        with open(reg_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Normalize
        feeds = data.get("feeds") or []
        if not isinstance(feeds, list):
            feeds = []
        return {"feeds": feeds}
    except Exception as e:
        _log.error("registry_load_failed", path=reg_path, error=str(e))
        raise


def validate_feeds(feeds: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
    """Validate feeds using strict required keys and warn on unknown keys.

    Required: id (str), type (str), cadence_seconds (int)
    Optional depending on type:
      - telegram: channel (str)
      - x: handle (str)
      - youtube_rss: channel_id (str)
    Returns (validated_feeds, warnings)
    """
    out: List[Dict[str, Any]] = []
    warnings: List[Dict[str, str]] = []
    for f in feeds:
        if not isinstance(f, dict):
            continue
        fid = f.get("id")
        ftype = f.get("type")
        cadence = f.get("cadence_seconds")
        if not fid or not isinstance(fid, str):
            warnings.append({"feed": str(f), "warning": "missing id"})
            _log.warning("registry_missing_id", feed=str(f))
            continue
        if not ftype or not isinstance(ftype, str):
            warnings.append({"feed": fid, "warning": "missing type"})
            _log.warning("registry_missing_type", feed=fid)
            continue
        if cadence is None or not isinstance(cadence, int):
            warnings.append({"feed": fid, "warning": "missing cadence_seconds"})
            _log.warning("registry_missing_cadence", feed=fid)
            continue
        known = {"id", "type", "cadence_seconds", "channel", "handle", "channel_id"}
        for k in list(f.keys()):
            if k not in known:
                warnings.append({"feed": fid, "warning": f"Unknown field '{k}' in feed '{fid}' – ignoring"})
                _log.warning("registry_unknown_field", field=k, feed=fid)
        # type-specific validation
        if ftype == "telegram":
            if not f.get("channel"):
                warnings.append({"feed": fid, "warning": "telegram requires 'channel'"})
                _log.warning("registry_missing_channel", feed=fid, type=ftype)
                continue
        elif ftype == "x":
            if not f.get("handle"):
                warnings.append({"feed": fid, "warning": "x requires 'handle'"})
                _log.warning("registry_missing_handle", feed=fid, type=ftype)
                continue
        elif ftype == "youtube_rss":
            if not f.get("channel_id"):
                warnings.append({"feed": fid, "warning": "youtube_rss requires 'channel_id'"})
                _log.warning("registry_missing_channel_id", feed=fid, type=ftype)
                continue
        # Keep only allowed keys
        cleaned: Dict[str, Any] = {
            "id": fid,
            "type": ftype,
            "cadence_seconds": cadence,
        }
        if ftype == "telegram":
            cleaned["channel"] = f.get("channel")
        if ftype == "x":
            cleaned["handle"] = f.get("handle")
        if ftype == "youtube_rss":
            cleaned["channel_id"] = f.get("channel_id")
        out.append(cleaned)
    return out, warnings


def convert_to_sources(feeds: List[Dict[str, Any]], registry_name: str = "single") -> Dict[str, Any]:
    """Convert validated feeds to FetcherHub sources.json schema.

    Maps into { registries: { <registry_name>: { live: { telegram/x/youtube } } } }
    Cadence is preserved per-feed in a meta field for future schedulers.
    """
    live: Dict[str, Any] = {
        "telegram": {"channels": [], "limit": 20},
        "x": {"handles": [], "limit": 20},
        "youtube": {"channel_ids": [], "limit": 20},
    }
    for f in feeds:
        t = f.get("type")
        if t == "telegram":
            live["telegram"]["channels"].append(f.get("channel"))
        elif t == "x":
            live["x"]["handles"].append(f.get("handle"))
        elif t == "youtube_rss":
            live["youtube"]["channel_ids"].append(f.get("channel_id"))
        # stash cadence for later schedulers (unused today)
        # could be stored in side-car or meta; for now include in a map
    sources = {
        "registries": {
            registry_name: {
                "live": live,
            }
        }
    }
    return sources


def write_sources_json(sources: Dict[str, Any]) -> str:
    _ensure_dirs()
    try:
        with open(SOURCES_JSON_PATH, "w", encoding="utf-8") as f:
            json.dump(sources, f, ensure_ascii=False, indent=2)
        _log.info("sources_json_updated", file=SOURCES_JSON_PATH)
        return SOURCES_JSON_PATH
    except Exception as e:
        _log.error("sources_json_write_failed", file=SOURCES_JSON_PATH, error=str(e))
        raise


def save_registry_yaml(feeds: List[Dict[str, Any]], path: Optional[str] = None) -> str:
    reg_path = os.path.abspath(path or DEFAULT_REGISTRY_PATH)
    if yaml is None:
        raise RuntimeError("pyyaml not installed; please install 'PyYAML' to use YAML registry")
    try:
        os.makedirs(os.path.dirname(reg_path), exist_ok=True)
        with open(reg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump({"feeds": feeds}, f, allow_unicode=True, sort_keys=False)
        _log.info("registry_yaml_saved", path=reg_path, feeds=len(feeds))
        return reg_path
    except Exception as e:
        _log.error("registry_yaml_save_failed", path=reg_path, error=str(e))
        raise


def hot_reload(registry_name: str = "single", path: Optional[str] = None) -> Dict[str, Any]:
    """Re-read YAML registry, validate, convert, and update sources.json for FetcherHub.

    Returns summary including warnings.
    """
    data = load_registry(path)
    feeds = data.get("feeds") or []
    validated, warnings = validate_feeds(feeds)
    sources = convert_to_sources(validated, registry_name=registry_name)
    out = write_sources_json(sources)
    return {"result": "ok", "sources_file": out, "feeds": len(validated), "warnings": warnings}