import os
import json
import uuid
import time
from typing import Any, Dict, Optional


def _traces_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "traces"))


SENSITIVE_KEYS = {"user_id", "handle", "email", "ip"}


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("***" if k in SENSITIVE_KEYS else _redact(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_redact(v) for v in obj]
    return obj


class TraceLogger:
    def __init__(self, retention_days: int = 7):
        self.retention_days = retention_days
        os.makedirs(_traces_root(), exist_ok=True)
        self._maybe_cleanup()

    def _maybe_cleanup(self) -> None:
        # Best-effort cleanup: remove files older than retention
        cutoff = time.time() - self.retention_days * 86400
        root = _traces_root()
        try:
            for name in os.listdir(root):
                path = os.path.join(root, name)
                try:
                    if os.path.isfile(path) and os.path.getmtime(path) < cutoff:
                        os.remove(path)
                except Exception:
                    continue
        except Exception:
            pass

    def log(self, stage: str, input_payload: Optional[Dict[str, Any]] = None, output_payload: Optional[Dict[str, Any]] = None, status: str = "running") -> str:
        trace_id = str(uuid.uuid4())
        entry = {
            "trace_id": trace_id,
            "stage": stage,
            "input": _redact(input_payload or {}),
            "output": _redact(output_payload or {}),
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": status,
        }
        path = os.path.join(_traces_root(), f"{stage}.jsonl")
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        return trace_id