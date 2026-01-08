import os
import json
import math
import time
from typing import Any, Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from .agents.script_gen_agent import ScriptGenAgent
from .agents.tts_agent_stub import TTSAgentStub
from .agents.avatar_agent_stub import AvatarAgentStub
from .logging_utils import StageLogger, PipelineLogger
from .trace_utils import TraceLogger


# Routing configuration (from user specification)
ROUTING_TABLE: Dict[Tuple[str, str], str] = {
    ("hi", "youth"): "HI-YOUTH",
    ("hi", "news"): "HI-NEWS",
    ("en", "news"): "EN-NEWS",
    ("en", "kids"): "EN-KIDS",
    ("ta", "news"): "TA-NEWS",
    ("bn", "news"): "BN-NEWS",
    # Mappings for new styles (fallback to news/standard buckets)
    ("en", "formal"): "EN-NEWS",
    ("en", "devotional"): "EN-NEWS",  # or a specific bucket if created later
    ("hi", "formal"): "HI-NEWS",
    ("hi", "devotional"): "HI-NEWS",
    ("ta", "formal"): "TA-NEWS",
    ("bn", "formal"): "BN-NEWS",
}
DEFAULT_BUCKET = "EN-NEWS"


PRIORITY_ORDER = [
    "HI-NEWS",
    "EN-NEWS",
    "HI-YOUTH",
    "EN-KIDS",
    "TA-NEWS",
    "BN-NEWS",
]


def _output_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "output"))


def _data_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))


def _read_json_list(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        return data.get("items") or []
    except Exception:
        return []


def _write_json(path: str, payload: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _record_dead_letter(stage: str, error_type: str, error_message: str, payload: Dict[str, Any]) -> None:
    root = os.path.join(_data_root(), "dead_letter")
    os.makedirs(root, exist_ok=True)
    path = os.path.join(root, f"{stage}.jsonl")
    rec = {
        "stage": stage,
        "error_type": error_type,
        "error_message": error_message,
        "payload": payload,
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")


def _route_bucket(lang: str, tone: str) -> str:
    key = (lang or "en").lower(), (tone or "news").lower()
    return ROUTING_TABLE.get(key, DEFAULT_BUCKET)


def _split_into_shards(items: List[Dict[str, Any]], shards: int) -> List[List[Dict[str, Any]]]:
    if shards <= 1 or len(items) == 0:
        return [items]
    chunk = max(1, math.ceil(len(items) / shards))
    return [items[i : i + chunk] for i in range(0, len(items), chunk)]


class BucketOrchestrator:
    def __init__(self, registry: str = "single", category: str = "general"):
        self.registry = registry
        self.category = category
        cores = os.cpu_count() or 4
        self.max_global_workers = min(4, cores)
        self.per_bucket_max = {
            "EN-NEWS": 2,
            "HI-NEWS": 1,
            "HI-YOUTH": 1,
            "EN-KIDS": 1,
            "TA-NEWS": 1,
            "BN-NEWS": 1,
        }
        self.log = PipelineLogger(component="bucket_orchestrator")
        self.stage_logger = StageLogger(source="pipeline", category=category, meta={"registry": registry})
        self.traces = TraceLogger(retention_days=7)

    def _write_stage_outputs(self, bucket: str, suffix: str, payload: Any) -> str:
        root = _output_root()
        os.makedirs(root, exist_ok=True)
        path = os.path.join(root, f"{self.registry}_{bucket}_{suffix}.json")
        _write_json(path, payload)
        return path

    def _retry(self, fn, stage: str, payload: Dict[str, Any]):
        delay = 1.0
        for attempt in range(3):
            try:
                return fn()
            except Exception as e:
                if attempt == 2:
                    _record_dead_letter(stage, type(e).__name__, str(e), payload)
                    raise
                time.sleep(delay)
                delay *= 2

    def _process_bucket(self, bucket: str, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        # Generate scripts
        self.stage_logger.start("summarize")
        agent_scripts = ScriptGenAgent(logger=self.log)
        self.traces.log("ScriptGenAgent", input_payload={"bucket": bucket, "items_count": len(items)}, status="running")
        scripts = self._retry(
            lambda: agent_scripts.generate(items, category=self.category),
            "scripts",
            {"bucket": bucket, "count": len(items)},
        )
        scripts_path = self._write_stage_outputs(bucket, "scripts", scripts)
        self.stage_logger.complete("summarize", meta={"bucket": bucket, "count": len(scripts), "file": scripts_path})
        self.traces.log("ScriptGenAgent", input_payload={"bucket": bucket}, output_payload={"scripts_count": len(scripts), "file": scripts_path}, status="success")

        # Synthesize voice
        self.stage_logger.start("voice")
        # Simple voice routing by bucket family
        if bucket.startswith("HI-"):
            voice = "hi-IN-AaravNeural"
        elif bucket.startswith("TA-"):
            voice = "ta-IN-PriyaNeural"
        elif bucket.startswith("BN-"):
            voice = "bn-IN-NabanitaNeural"
        elif bucket.startswith("EN-KIDS"):
            voice = "en-US-Kids-1"
        else:
            voice = "en-US-Neural-1"
        agent_tts = TTSAgentStub(voice=voice, logger=self.log)
        self.traces.log("TTSAgent", input_payload={"bucket": bucket, "voice": voice, "scripts_count": len(scripts)}, status="running")
        voice_items = self._retry(
            lambda: agent_tts.synthesize(scripts, category=self.category),
            "voice",
            {"bucket": bucket, "count": len(scripts)},
        )
        voice_path = self._write_stage_outputs(bucket, "voice", voice_items)
        self.stage_logger.complete("voice", meta={"bucket": bucket, "count": len(voice_items), "file": voice_path})
        self.traces.log("TTSAgent", input_payload={"bucket": bucket}, output_payload={"voice_count": len(voice_items), "file": voice_path}, status="success")

        # Filter valid voice items for avatar rendering
        valid_voice_items = [v for v in voice_items if v.get("stage_status", {}).get("voice") == "success" and v.get("audio_path")]

        # Render avatar
        self.stage_logger.start("avatar")
        style = (
            "news-anchor"
            if bucket.endswith("NEWS")
            else ("youth-vlogger" if bucket.endswith("YOUTH") else "kids-host")
        )
        agent_avatar = AvatarAgentStub(style=style, logger=self.log)
        self.traces.log("AvatarAgent", input_payload={"bucket": bucket, "style": style, "voice_count": len(valid_voice_items)}, status="running")
        videos = self._retry(
            lambda: agent_avatar.render(valid_voice_items, category=self.category),
            "avatar",
            {"bucket": bucket, "count": len(valid_voice_items)},
        )
        avatar_path = self._write_stage_outputs(bucket, "avatar", videos)
        self.stage_logger.complete("avatar", meta={"bucket": bucket, "count": len(videos), "file": avatar_path})
        self.traces.log("AvatarAgent", input_payload={"bucket": bucket}, output_payload={"videos_count": len(videos), "file": avatar_path}, status="success")

        return {
            "bucket": bucket,
            "counts": {"items": len(items), "scripts": len(scripts), "voice": len(voice_items), "avatar": len(videos)},
            "files": {"scripts": scripts_path, "voice": voice_path, "avatar": avatar_path},
        }

    def run(self) -> Dict[str, Any]:
        filtered_path = os.path.join(_output_root(), f"{self.registry}_filtered.json")
        items = _read_json_list(filtered_path)
        if not items:
            return {"success": False, "message": "No filtered items found", "file": filtered_path}

        # Route items to buckets
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for it in items:
            lang = (it.get("language") or "en").lower()
            tone = (it.get("tone") or "news").lower()
            b = _route_bucket(lang, tone)
            buckets.setdefault(b, []).append(it)

        # Prepare shard tasks according to per-bucket limits and priority order
        tasks = []
        for b in PRIORITY_ORDER:
            b_items = buckets.get(b, [])
            if not b_items:
                continue
            shards = self.per_bucket_max.get(b, 1)
            for shard_items in _split_into_shards(b_items, shards):
                tasks.append((b, shard_items))

        results = []
        self.stage_logger.start("bucket_orchestration")
        with ThreadPoolExecutor(max_workers=self.max_global_workers) as ex:
            future_to_bucket = {ex.submit(self._process_bucket, b, shard_items): b for (b, shard_items) in tasks}
            for fut in as_completed(future_to_bucket):
                bucket = future_to_bucket[fut]
                try:
                    res = fut.result()
                    res["status"] = "success"
                    results.append(res)
                except Exception as e:
                    # Capture failure in results
                    self.log.error("bucket_task_failed", bucket=bucket, error=str(e))
                    results.append({
                        "bucket": bucket,
                        "status": "failed",
                        "error": str(e)
                    })
                    continue
        
        # Determine overall success
        success_count = sum(1 for r in results if r.get("status") != "failed")
        overall_success = (success_count > 0)
        
        self.stage_logger.complete("bucket_orchestration", meta={"buckets_total": len(tasks), "buckets_success": success_count})
        self.stage_logger.end_run("completed")
        return {
            "success": overall_success, 
            "buckets_total": len(tasks),
            "buckets_success": success_count,
            "results": results
        }