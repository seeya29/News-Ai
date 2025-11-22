import logging
import sys
import json
import os
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    # Local import; used only when running within the project
    from server import db as server_db
except Exception:
    server_db = None


class PipelineLogger:
    def __init__(self, component: str = "pipeline", level: int = logging.INFO):
        self.component = component
        self._logger = logging.getLogger("news_ai")
        self._logger.setLevel(level)
        if not any(isinstance(h, logging.StreamHandler) for h in self._logger.handlers):
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(level)
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            self._logger.addHandler(handler)
        # File logging: structured JSON, 7-day retention, rotate daily
        if not any(isinstance(h, TimedRotatingFileHandler) for h in self._logger.handlers):
            try:
                logs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "logs"))
                os.makedirs(logs_dir, exist_ok=True)
                file_path = os.path.join(logs_dir, "app.log")
                fh = TimedRotatingFileHandler(file_path, when="D", interval=1, backupCount=7, encoding="utf-8")
                fh.setLevel(level)
                fh.setFormatter(logging.Formatter("%(message)s"))
                self._logger.addHandler(fh)
            except Exception:
                # Best-effort; if file handler fails, continue with stdout
                pass

    def _emit(self, level: int, event: str, **fields):
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": logging.getLevelName(level),
            "component": self.component,
            "event": event,
            **{k: v for k, v in fields.items() if v is not None},
        }
        self._logger.log(level, json.dumps(payload, ensure_ascii=False))

    def info(self, event: str, **fields):
        self._emit(logging.INFO, event, **fields)

    def warning(self, event: str, **fields):
        self._emit(logging.WARNING, event, **fields)

    def error(self, event: str, **fields):
        self._emit(logging.ERROR, event, **fields)


def get_logger(component: str = "pipeline", level: int = logging.INFO) -> PipelineLogger:
    return PipelineLogger(component=component, level=level)


class StageLogger:
    """DB-backed stage visibility logger.

    Emits pipeline run and per-stage events to the database. Safe no-op if DB is unavailable.
    """

    def __init__(self, run_id: Optional[str] = None, source: Optional[str] = None, category: Optional[str] = None, meta: Optional[Dict[str, Any]] = None):
        self.run_id = run_id or f"run|{datetime.now(timezone.utc).isoformat()}|{category or 'general'}"
        self.source = source or "pipeline"
        self.category = category or "general"
        self.meta = meta or {}
        self._stage_start_ts: Dict[str, float] = {}

        # Start the run
        self._upsert_run({
            "run_id": self.run_id,
            "source": self.source,
            "category": self.category,
            "status": "running",
            "started_at": datetime.now(timezone.utc).isoformat(),
            "ended_at": None,
            "meta": self.meta,
        })

    def _upsert_run(self, run: Dict[str, Any]) -> None:
        if server_db:
            try:
                server_db.upsert_pipeline_run(run)
            except Exception:
                pass

    def _upsert_stage(self, ev: Dict[str, Any]) -> None:
        if server_db:
            try:
                server_db.upsert_stage_event(ev)
            except Exception:
                pass

    def start(self, stage: str, meta: Optional[Dict[str, Any]] = None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        self._stage_start_ts[stage] = datetime.now(timezone.utc).timestamp()
        self._upsert_stage({
            "run_id": self.run_id,
            "stage": stage,
            "status": "running",
            "progress": 0,
            "started_at": ts,
            "ended_at": None,
            "duration_ms": None,
            "meta": meta or {},
        })

    def update(self, stage: str, progress: Optional[int] = None, meta: Optional[Dict[str, Any]] = None) -> None:
        self._upsert_stage({
            "run_id": self.run_id,
            "stage": stage,
            "status": "running",
            "progress": progress,
            "meta": meta or {},
        })

    def error(self, stage: str, error_code: str, error_message: str, meta: Optional[Dict[str, Any]] = None) -> None:
        end_ts = datetime.now(timezone.utc).timestamp()
        started_ts = self._stage_start_ts.get(stage)
        dur_ms = int((end_ts - started_ts) * 1000) if started_ts else None
        self._upsert_stage({
            "run_id": self.run_id,
            "stage": stage,
            "status": "failed",
            "progress": None,
            "error_code": error_code,
            "error_message": error_message,
            "started_at": None,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "duration_ms": dur_ms,
            "meta": meta or {},
        })
        # Mark run failed
        self._upsert_run({
            "run_id": self.run_id,
            "source": self.source,
            "category": self.category,
            "status": "failed",
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "meta": self.meta,
        })

    def complete(self, stage: str, meta: Optional[Dict[str, Any]] = None) -> None:
        end_ts = datetime.now(timezone.utc).timestamp()
        started_ts = self._stage_start_ts.get(stage)
        dur_ms = int((end_ts - started_ts) * 1000) if started_ts else None
        self._upsert_stage({
            "run_id": self.run_id,
            "stage": stage,
            "status": "completed",
            "progress": 100,
            "started_at": None,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "duration_ms": dur_ms,
            "meta": meta or {},
        })

    def end_run(self, status: str = "completed") -> None:
        self._upsert_run({
            "run_id": self.run_id,
            "source": self.source,
            "category": self.category,
            "status": status,
            "ended_at": datetime.now(timezone.utc).isoformat(),
            "meta": self.meta,
        })