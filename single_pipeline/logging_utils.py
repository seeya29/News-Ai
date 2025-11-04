from typing import Any, Dict, Optional


class PipelineLogger:
    def __init__(self):
        pass

    def log_event(self, stage: str, payload: Optional[Dict[str, Any]] = None):
        msg = {"stage": stage, "payload": payload or {}}
        print(f"[pipeline] {msg}")