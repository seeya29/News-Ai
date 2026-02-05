import os
import logging
import glob
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import base64
import jwt
from single_pipeline.cli import run_fetch, run_filter, run_scripts, run_voice, run_avatar
from single_pipeline.agents.tts_agent_stub import TTSAgentStub
from single_pipeline.agents.avatar_agent_stub import AvatarAgentStub
from single_pipeline.rag_client import RAGClient
from single_pipeline.debug.langgraph_stub import build_graph_from_traces
from single_pipeline.registry import (
    DEFAULT_REGISTRY_PATH,
    load_registry,
    validate_feeds,
    save_registry_yaml,
    hot_reload,
)
from server.db import (
    init_db,
    get_user_preferences as db_get_user_prefs,
    upsert_user_preferences as db_upsert_user_prefs,
    insert_user_feedback as db_insert_feedback,
    get_articles as db_get_articles,
    count_articles as db_count_articles,
    count_article_groups as db_count_article_groups,
    get_grouped_articles as db_get_grouped_articles,
    get_article_by_id as db_get_article_by_id,
    get_group_representative as db_get_group_representative,
    get_articles_in_timeframe as db_get_articles_in_timeframe,
    upsert_article as db_upsert_article,
)


APP = FastAPI(title="News-Ai API", version="0.1.0")
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass
log = logging.getLogger("server.app")
if not log.handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# --------------------
# Authentication (JWT, verified decode)
# --------------------
security = HTTPBearer(auto_error=True)
JWT_ALG = os.getenv("JWT_ALG", "HS256").upper()
JWT_SECRET = os.getenv("JWT_SECRET")
ALLOWED_JWT_ALGS = {"HS256"}
if JWT_ALG not in ALLOWED_JWT_ALGS:
    JWT_ALG = "HS256"
CDN_BASE_URL = os.getenv("CDN_BASE_URL", "https://cdn.newsai.com")
SOURCE_LOGO_URL = os.getenv("SOURCE_LOGO_URL", f"{CDN_BASE_URL}/sources/news-ai.png")
DEFAULT_THUMBNAIL_URL = os.getenv("DEFAULT_THUMBNAIL_URL", f"{CDN_BASE_URL}/thumbs/default.jpg")
def _sanitize_identifier(s: Optional[str]) -> str:
    s = (s or "").strip()
    import re as _re
    cleaned = _re.sub(r"[^a-zA-Z0-9_-]", "", s)
    return cleaned or "default"
def _safe_join(root: str, relative: str) -> str:
    root_abs = os.path.abspath(root)
    cand = os.path.abspath(os.path.join(root_abs, relative))
    if cand == root_abs or cand.startswith(root_abs + os.sep):
        return cand
    raise HTTPException(status_code=400, detail="invalid_path")


class AuthContext(BaseModel):
    user_id: str
    role: Optional[str] = "user"
    exp: int


def _decode_jwt_payload(token: str) -> Dict[str, Any]:
    # Enforce signature verification and disallow alg=none
    if not JWT_SECRET:
        raise HTTPException(status_code=401, detail="unauthorized")
    try:
        header = jwt.get_unverified_header(token) or {}
        if str(header.get("alg", "")).lower() == "none":
            raise jwt.InvalidAlgorithmError("alg none not allowed")
        return jwt.decode(
            token,
            JWT_SECRET,
            algorithms=[JWT_ALG],
            options={"require": ["exp", "user_id"], "verify_signature": True},
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="token_expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="invalid_token")


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(security)) -> AuthContext:
    claims = _decode_jwt_payload(credentials.credentials)
    user_id = claims.get("user_id")
    role = claims.get("role") or "user"
    exp = claims.get("exp")
    if not user_id or not exp:
        raise HTTPException(status_code=401, detail="invalid_token")
    return AuthContext(user_id=user_id, role=role, exp=int(exp))


# --------------------
# Generic per-key rate limiter with headers
# --------------------
def _error(code: str, status: int, message: str, details: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> JSONResponse:
    payload = {
        "error": code,
        "message": message,
        "details": details or {},
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return JSONResponse(status_code=status, content=payload, headers=headers)


def _apply_rate_limit(buckets: Dict[str, Dict[str, Any]], limit: int, key: str):
    now = int(time.time())
    window = now // 60
    # Cleanup old buckets and enforce global size cap for this bucket set
    try:
        stale = [k for k, b in buckets.items() if b.get("window", window) < window - 1]
        for k in stale:
            buckets.pop(k, None)
        if 'RATE_BUCKET_MAX_ENTRIES' in globals():
            max_entries = RATE_BUCKET_MAX_ENTRIES
            if len(buckets) >= max_entries:
                oldest = sorted(buckets.items(), key=lambda kv: kv[1].get("window", window))[: max(1, max_entries // 10)]
                for k, _ in oldest:
                    buckets.pop(k, None)
    except Exception:
        # Be defensive: rate limiting should never crash a request
        pass
    bucket = buckets.get(key)
    if not bucket or bucket.get("window") != window:
        buckets[key] = {"window": window, "count": 0}
        bucket = buckets[key]
    reset = (window + 1) * 60
    if bucket["count"] >= limit:
        headers = {
            "X-RateLimit-Limit": str(limit),
            "X-RateLimit-Remaining": "0",
            "X-RateLimit-Reset": str(reset),
        }
        return _error("rate_limit_exceeded", 429, f"Max {limit} requests per minute", details={"reset": reset}, headers=headers), None
    bucket["count"] += 1
    remaining = max(0, limit - bucket["count"])
    return None, {"limit": limit, "remaining": remaining, "reset": reset}


# --------------------
# Exception handlers (standardize error format)
# --------------------
@APP.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return _error("validation_error", 400, "Request data invalid", details={"errors": exc.errors()})


@APP.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Map common statuses to codes when detail is not a dict
    code_map = {
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        429: "rate_limit_exceeded",
        400: "bad_request",
    }
    detail = exc.detail
    if isinstance(detail, dict):
        code = detail.get("error") or code_map.get(exc.status_code, "error")
        message = detail.get("message") or (detail if isinstance(detail, str) else "") or "Error"
        details = {k: v for k, v in detail.items() if k not in ("error", "message")}
        return _error(code, exc.status_code, message, details=details)
    else:
        code = code_map.get(exc.status_code, "error")
        message = str(detail) if detail else "Error"
        return _error(code, exc.status_code, message)


# Simple per-user rate limiter: 60 requests/minute
RATE_LIMIT_PER_MINUTE = 60
_rate_buckets: Dict[str, Dict[str, Any]] = {}
RATE_BUCKET_MAX_ENTRIES = int(os.getenv("RATE_BUCKET_MAX_ENTRIES", "5000"))


def _check_rate_limit(user_id: str) -> Optional[JSONResponse]:
    now = int(time.time())
    window = now // 60
    # Cleanup old buckets and enforce size cap
    stale = [uid for uid, b in _rate_buckets.items() if b.get("window", window) < window - 1]
    for uid in stale:
        _rate_buckets.pop(uid, None)
    if len(_rate_buckets) >= RATE_BUCKET_MAX_ENTRIES:
        oldest = sorted(_rate_buckets.items(), key=lambda kv: kv[1].get("window", window))[: max(1, RATE_BUCKET_MAX_ENTRIES // 10)]
        for uid, _ in oldest:
            _rate_buckets.pop(uid, None)
    bucket = _rate_buckets.get(user_id)
    if not bucket:
        _rate_buckets[user_id] = {"window": window, "count": 1}
        return None
    if bucket["window"] != window:
        _rate_buckets[user_id] = {"window": window, "count": 1}
        return None
    if bucket["count"] >= RATE_LIMIT_PER_MINUTE:
        return _error("rate_limit_exceeded", 429, f"Max {RATE_LIMIT_PER_MINUTE} requests per minute")
    bucket["count"] += 1
    return None


# 5-minute TTL cache per user+params
CACHE_TTL_SECONDS = 300
FEED_CACHE_MAX_ENTRIES = int(os.getenv("FEED_CACHE_MAX_ENTRIES", "1000"))
_feed_cache: Dict[str, Dict[str, Any]] = {}


def _cache_key(user_id: str, limit: int, page: int, category: Optional[str]) -> str:
    return f"{user_id}|{limit}|{page}|{category or ''}"


def _get_cached_response(key: str) -> Optional[Dict[str, Any]]:
    entry = _feed_cache.get(key)
    if not entry:
        return None
    if time.time() > entry["expires"]:
        _feed_cache.pop(key, None)
        return None
    return entry["data"]


def _set_cache(key: str, data: Dict[str, Any]) -> None:
    # Cleanup expired entries and enforce max size
    now = time.time()
    expired_keys = [k for k, v in _feed_cache.items() if now > v.get("expires", 0)]
    for k in expired_keys:
        _feed_cache.pop(k, None)
    if len(_feed_cache) >= FEED_CACHE_MAX_ENTRIES:
        # Evict oldest by expires timestamp
        oldest = sorted(_feed_cache.items(), key=lambda kv: kv[1].get("expires", now))[: max(1, FEED_CACHE_MAX_ENTRIES // 10)]
        for k, _ in oldest:
            _feed_cache.pop(k, None)
    _feed_cache[key] = {"expires": now + CACHE_TTL_SECONDS, "data": data}


def _iso(dt: Optional[float]) -> str:
    if dt is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        # Assume seconds since epoch
        return datetime.fromtimestamp(float(dt), tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _parse_epoch(ts: Optional[str]) -> float:
    try:
        if not ts:
            return time.time()
        return datetime.fromisoformat(ts).timestamp()
    except Exception:
        return time.time()


def _safe_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    try:
        return json.dumps(x, ensure_ascii=False)
    except Exception:
        return str(x)


def _reading_time_minutes(text: str) -> int:
    words = len(text.split())
    # ~200 wpm average
    return max(1, int(words / 200) or 1)


def _hash_id(title: str, body: str) -> str:
    h = hashlib.sha256((title + "|" + body).encode("utf-8", errors="ignore")).hexdigest()
    return f"article_{h[:12]}"


def _load_items_from_output(category: Optional[str]) -> List[Dict[str, Any]]:
    root = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output")
    root = os.path.abspath(root)
    items: List[Dict[str, Any]] = []
    rag = RAGClient()
    for path in glob.glob(os.path.join(root, "*_items.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for it in data:
                    title = _safe_str(it.get("title"))
                    body = _safe_str(it.get("body"))
                    ts = it.get("timestamp")
                    item_cat = (it.get("category") or (category or "general")).lower()
                    published_iso = _iso(ts)
                    # RAG dedup flags + group key assignment
                    try:
                        dedup_flag = rag.is_duplicate(title or "Untitled", body or "")
                    except Exception:
                        dedup_flag = False
                    try:
                        group_key = rag.assign_group_key(title or "Untitled", body or "", published_iso, item_cat)
                    except Exception:
                        group_key = None
                    # build response shape
                    items.append({
                        "id": _hash_id(title, body),
                        "title": title or "Untitled",
                        "source": {
                            "name": "News-Ai",
                            "logo_url": SOURCE_LOGO_URL,
                        },
                        "metadata": {
                            "published_at": published_iso,
                            "category": item_cat,
                            "reading_time_minutes": _reading_time_minutes(body),
                            "group_key": group_key,
                        },
                        "relevance_score": 0.75,
                        "thumbnail_url": DEFAULT_THUMBNAIL_URL,
                        "processing_status": ("deduped" if dedup_flag else "ingested"),
                        "processing_progress": 50,
                        "processing_stage": "Verify",
                    })
        except Exception as e:
            log.warning("items_load_failed", extra=None)
            try:
                log.warning(f"Failed to load items from {path}: {e}")
            except Exception:
                pass
            continue
    # Sort newest first by published_at
    def _parse_ts(x: Dict[str, Any]) -> float:
        try:
            return datetime.fromisoformat(x["metadata"]["published_at"]).timestamp()
        except Exception:
            return 0.0
    items.sort(key=_parse_ts, reverse=True)
    # Optional filter by category if provided
    if category:
        items = [i for i in items if i["metadata"]["category"].lower() == category.lower()]
    return items


def _map_row_to_feed_item(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": row.get("id"),
        "title": row.get("title") or "Untitled",
        "source": {
            "name": row.get("source_name") or "News-Ai",
            "logo_url": SOURCE_LOGO_URL,
        },
        "metadata": {
            "published_at": row.get("published_at") or datetime.now(timezone.utc).isoformat(),
            "category": (row.get("category") or "general").lower(),
            "reading_time_minutes": 3,
        },
        "relevance_score": float(row.get("relevance_score") or 0.75),
        "thumbnail_url": row.get("thumbnail_url") or DEFAULT_THUMBNAIL_URL,
        "processing_status": row.get("processing_status") or "ingested",
        "processing_progress": int(row.get("processing_progress") or 10),
        "processing_stage": "Verify",
    }


def _load_items_from_db(category: Optional[str], limit: int, page: int) -> Dict[str, Any]:
    # Grouped feed: count distinct groups and fetch one representative per group
    total_groups = db_count_article_groups(category)
    if total_groups == 0:
        return {"articles": [], "total": 0}
    offset = (page - 1) * limit
    rows = db_get_grouped_articles(limit=limit, offset=offset, category=category)
    items = [_map_row_to_feed_item(r) for r in rows]
    return {"articles": items, "total": total_groups}


@APP.get("/api/articles/feed/{user_id}")
def get_personalized_feed(user_id: str, limit: int = 20, page: int = 1, category: Optional[str] = None, response: Response = None, auth: AuthContext = Depends(require_auth)):
    # Auth: user can only access own feed unless admin
    if auth.user_id != user_id and (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")

    # Rate limiting per user
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    # Cache lookup
    key = _cache_key(user_id, limit, page, category)
    cached = _get_cached_response(key)
    if cached is not None:
        return cached

    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
    if page <= 0:
        raise HTTPException(status_code=400, detail="page must be >= 1")
    # Category validation (simple supported set)
    if category:
        allowed_categories = {"general", "finance", "tech", "science"}
        if category.lower() not in allowed_categories:
            return _error("invalid_category", 400, "Category not supported", details={"category": category})

    # Prefer DB-backed grouped items, fallback to file-backed if DB is empty
    db_payload = _load_items_from_db(category, limit, page)
    if db_payload["total"] > 0:
        page_items = db_payload["articles"]
        total = db_payload["total"]
    else:
        items = _load_items_from_output(category)
        total = len(items)
        start = (page - 1) * limit
        end = start + limit
        page_items = items[start:end]

    # Compute has_next robustly even for DB-backed path
    has_next = ((page - 1) * limit + len(page_items)) < total
    response = {
        "articles": page_items,
        "meta": {
            "total_count": total,
            "current_page": page,
            "has_next": has_next,
        },
    }

    _set_cache(key, response)
    return response


# Health endpoint
@APP.get("/api/health")
def health(auth: AuthContext = Depends(require_auth)):
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat(), "user": auth.user_id}


# --------------------
# Feedback endpoint
# --------------------

# Separate rate limiter for feedback: 120 requests/minute per user
FEEDBACK_RATE_LIMIT_PER_MINUTE = 120
_feedback_rate_buckets: Dict[str, Dict[str, Any]] = {}


def _check_feedback_rate_limit(user_id: str) -> Optional[JSONResponse]:
    now = int(time.time())
    bucket = _feedback_rate_buckets.get(user_id)
    if not bucket:
        _feedback_rate_buckets[user_id] = {"window": now // 60, "count": 1}
        return None
    window = now // 60
    if bucket["window"] != window:
        _feedback_rate_buckets[user_id] = {"window": window, "count": 1}
        return None
    if bucket["count"] >= FEEDBACK_RATE_LIMIT_PER_MINUTE:
        return _error("rate_limit_exceeded", 429, f"Max {FEEDBACK_RATE_LIMIT_PER_MINUTE} requests per minute")
    bucket["count"] += 1
    return None


class FeedbackContext(BaseModel):
    time_on_page: Optional[float] = Field(default=None, description="Seconds spent on page")
    scroll_depth: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="0.0 to 1.0")
    device: Optional[str] = Field(default=None, description="mobile|tablet|desktop")
    session_id: Optional[str] = None


class ArticleFeedbackRequest(BaseModel):
    user_id: str
    article_id: str
    action: str
    timestamp: Optional[str] = None
    context: Optional[FeedbackContext] = None


_article_index: Dict[str, Dict[str, Any]] = {}
_article_index_expires: float = 0.0


def _build_article_index() -> None:
    global _article_index, _article_index_expires
    items = _load_items_from_output(None)
    _article_index = {i["id"]: i for i in items}
    _article_index_expires = time.time() + 300  # 5 minutes


def _find_article_by_id(article_id: str) -> Optional[Dict[str, Any]]:
    # Prefer DB-backed lookup for performance; fallback to file-backed index
    try:
        row = db_get_article_by_id(article_id)
        if row:
            return row
    except Exception as e:
        log.warning("db_article_lookup_failed")
    global _article_index, _article_index_expires
    if time.time() > _article_index_expires or not _article_index:
        _build_article_index()
    return _article_index.get(article_id)


def _reward_for_action(action: str) -> Optional[int]:
    mapping = {
        "like": 1,
        "save": 1,
        "share": 1,
        "dislike": -1,
        "skip": -1,
    }
    return mapping.get(action)


def _update_relevance(base: float, reward: int) -> float:
    # Simple update: scale reward to a small delta, clamp [0,1]
    delta = 0.12 * float(reward)
    new_score = max(0.0, min(1.0, base + delta))
    return new_score


def _make_feedback_id(user_id: str, article_id: str, ts: Optional[str]) -> str:
    payload = f"{user_id}|{article_id}|{ts or ''}|{time.time()}"
    h = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"feedback_{h[:12]}"


_feedback_events: List[Dict[str, Any]] = []
FEEDBACK_EVENTS_MAX_ENTRIES = int(os.getenv("FEEDBACK_EVENTS_MAX_ENTRIES", "10000"))
FEEDBACK_EVENTS_TTL_SECONDS = int(os.getenv("FEEDBACK_EVENTS_TTL_SECONDS", str(7 * 24 * 3600)))

def _prune_event_list(events: List[Dict[str, Any]], max_entries: int, ttl_seconds: int) -> None:
    try:
        now = time.time()
        if ttl_seconds > 0:
            cutoff = now - ttl_seconds
            events[:] = [e for e in events if _parse_epoch(e.get("timestamp")) >= cutoff]
        if max_entries > 0 and len(events) > max_entries:
            evict = max(1, max_entries // 10)
            # Sort oldest first by timestamp
            events.sort(key=lambda e: _parse_epoch(e.get("timestamp")))
            # Keep most recent (max_entries - evict)
            keep = max_entries - evict
            events[:] = events[-keep:]
    except Exception:
        # Never fail request due to pruning
        pass


@APP.post("/api/feedback/article")
def submit_article_feedback(payload: ArticleFeedbackRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    # Auth: payload user must match token unless admin
    if auth.user_id != payload.user_id and (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")

    # Rate limiting per user
    err, info = _apply_rate_limit(_feedback_rate_buckets, FEEDBACK_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    # Validate action
    reward = _reward_for_action(payload.action)
    if reward is None:
        return _error("invalid_action", 400, "Feedback action invalid", details={"action": payload.action})

    # Find article; if missing (e.g., preview items without IDs), create a stub in DB
    article = _find_article_by_id(payload.article_id)
    if not article:
        try:
            stub = {
                "id": payload.article_id,
                "title": None,
                "source_name": "UI Preview",
                "source_url": None,
                "thumbnail_url": None,
                "category": "general",
                "published_at": datetime.now(timezone.utc).isoformat(),
                "relevance_score": 0.75,
                "processing_status": "ingested",
                "processing_progress": 0,
                "group_key": None,
            }
            db_upsert_article(stub)
            article = stub
        except Exception as e:
            log.warning("article_stub_upsert_failed")
            article = {"id": payload.article_id, "relevance_score": 0.75}

    base_score = float(article.get("relevance_score", 0.75))
    updated_score = _update_relevance(base_score, reward)

    # Record feedback event in-memory (no caching on response, but we track events)
    event = {
        "feedback_id": _make_feedback_id(payload.user_id, payload.article_id, payload.timestamp),
        "user_id": payload.user_id,
        "article_id": payload.article_id,
        "action": payload.action,
        "timestamp": payload.timestamp or datetime.now(timezone.utc).isoformat(),
        "context": payload.context.dict() if payload.context else {},
        "reward": reward,
        "updated_relevance_score": updated_score,
    }
    _feedback_events.append(event)
    _prune_event_list(_feedback_events, FEEDBACK_EVENTS_MAX_ENTRIES, FEEDBACK_EVENTS_TTL_SECONDS)
    # Persist to DB (best-effort)
    try:
        db_insert_feedback(event)
    except Exception as e:
        log.warning("feedback_persist_failed")

    return {
        "success": True,
        "feedback_id": event["feedback_id"],
        "updated_relevance_score": updated_score,
        "message": "Feedback recorded successfully",
    }


# --------------------
# Engagement metrics endpoint
# --------------------

ENG_RATE_LIMIT_PER_MINUTE = 200
_eng_rate_buckets: Dict[str, Dict[str, Any]] = {}


def _check_eng_rate_limit(user_id: str) -> Optional[JSONResponse]:
    now = int(time.time())
    bucket = _eng_rate_buckets.get(user_id)
    if not bucket:
        _eng_rate_buckets[user_id] = {"window": now // 60, "count": 1}
        return None
    window = now // 60
    if bucket["window"] != window:
        _eng_rate_buckets[user_id] = {"window": window, "count": 1}
        return None
    if bucket["count"] >= ENG_RATE_LIMIT_PER_MINUTE:
        return _error("rate_limit_exceeded", 429, f"Max {ENG_RATE_LIMIT_PER_MINUTE} requests per minute")
    bucket["count"] += 1
    return None


class EngagementMetrics(BaseModel):
    total_time: float = Field(..., ge=0.0)
    scroll_depth: float = Field(..., ge=0.0, le=1.0)
    scroll_events: int = Field(..., ge=0)
    scroll_direction_changes: int = Field(..., ge=0)
    pause_count: int = Field(..., ge=0)
    pause_total_duration: float = Field(..., ge=0.0)
    links_clicked: int = Field(0, ge=0)
    completion_estimated: float = Field(..., ge=0.0, le=1.0)


class DeviceInfo(BaseModel):
    type: Optional[str] = Field(default=None, description="mobile|tablet|desktop")
    viewport_size: Optional[str] = None
    connection_type: Optional[str] = None


class EngagementEventRequest(BaseModel):
    user_id: str
    article_id: str
    session_id: str
    timestamp: Optional[str] = None
    engagement_metrics: EngagementMetrics
    device_info: Optional[DeviceInfo] = None


class EngagementBatchRequest(BaseModel):
    events: List[EngagementEventRequest]


_engagement_events: List[Dict[str, Any]] = []
ENGAGEMENT_EVENTS_MAX_ENTRIES = int(os.getenv("ENGAGEMENT_EVENTS_MAX_ENTRIES", "10000"))
ENGAGEMENT_EVENTS_TTL_SECONDS = int(os.getenv("ENGAGEMENT_EVENTS_TTL_SECONDS", str(7 * 24 * 3600)))


def _make_engagement_id(user_id: str, article_id: str, session_id: str, ts: Optional[str]) -> str:
    payload = f"{user_id}|{article_id}|{session_id}|{ts or ''}|{time.time()}"
    h = hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()
    return f"engagement_{h[:12]}"


def _quality_score(m: EngagementMetrics) -> float:
    # Normalize reading time against 120s baseline
    time_norm = min(m.total_time / 120.0, 1.0)
    base = 0.4 * m.scroll_depth + 0.4 * m.completion_estimated + 0.2 * time_norm
    penalty = 0.05 * min(m.scroll_direction_changes, 10) + 0.05 * min(m.pause_count, 10) + 0.0005 * m.pause_total_duration
    score = max(0.0, min(1.0, base - penalty))
    return round(score, 4)


@APP.post("/api/metrics/engagement")
def track_article_engagement(payload: dict, response: Response, auth: AuthContext = Depends(require_auth)):
    # Accept either single event or batch {"events": [...]}
    # Determine user_id for rate-limiting: first event or single
    try:
        if "events" in payload and isinstance(payload["events"], list) and payload["events"]:
            first = EngagementEventRequest(**payload["events"][0])
            # Auth: user must match token unless admin
            if auth.user_id != first.user_id and (auth.role or "user") != "admin":
                raise HTTPException(status_code=403, detail="forbidden")
            err, info = _apply_rate_limit(_eng_rate_buckets, ENG_RATE_LIMIT_PER_MINUTE, auth.user_id)
            if err is not None:
                return err
            response.headers["X-RateLimit-Limit"] = str(info["limit"])
            response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
            response.headers["X-RateLimit-Reset"] = str(info["reset"])
            results = []
            for ev_raw in payload["events"]:
                ev = EngagementEventRequest(**ev_raw)
                # Optional: article existence check (non-fatal)
                # article = _find_article_by_id(ev.article_id)
                # Compute quality score
                q = _quality_score(ev.engagement_metrics)
                eid = _make_engagement_id(ev.user_id, ev.article_id, ev.session_id, ev.timestamp)
                _engagement_events.append({
                    "engagement_id": eid,
                    "user_id": ev.user_id,
                    "article_id": ev.article_id,
                    "session_id": ev.session_id,
                    "timestamp": ev.timestamp or datetime.now(timezone.utc).isoformat(),
                    "metrics": ev.engagement_metrics.dict(),
                    "device": ev.device_info.dict() if ev.device_info else {},
                    "quality_score": q,
                })
                _prune_event_list(_engagement_events, ENGAGEMENT_EVENTS_MAX_ENTRIES, ENGAGEMENT_EVENTS_TTL_SECONDS)
                results.append({"engagement_id": eid, "quality_score": q})
            return {"success": True, "count": len(results), "engagement": results, "message": "Engagement metrics recorded"}
        else:
            ev = EngagementEventRequest(**payload)
            if auth.user_id != ev.user_id and (auth.role or "user") != "admin":
                raise HTTPException(status_code=403, detail="forbidden")
            err, info = _apply_rate_limit(_eng_rate_buckets, ENG_RATE_LIMIT_PER_MINUTE, auth.user_id)
            if err is not None:
                return err
            response.headers["X-RateLimit-Limit"] = str(info["limit"])
            response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
            response.headers["X-RateLimit-Reset"] = str(info["reset"])
            q = _quality_score(ev.engagement_metrics)
            eid = _make_engagement_id(ev.user_id, ev.article_id, ev.session_id, ev.timestamp)
            _engagement_events.append({
                "engagement_id": eid,
                "user_id": ev.user_id,
                "article_id": ev.article_id,
                "session_id": ev.session_id,
                "timestamp": ev.timestamp or datetime.now(timezone.utc).isoformat(),
                "metrics": ev.engagement_metrics.dict(),
                "device": ev.device_info.dict() if ev.device_info else {},
                "quality_score": q,
            })
            _prune_event_list(_engagement_events, ENGAGEMENT_EVENTS_MAX_ENTRIES, ENGAGEMENT_EVENTS_TTL_SECONDS)
            return {"success": True, "engagement_id": eid, "quality_score": q, "message": "Engagement metrics recorded"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --------------------
# Voice/Avatar endpoints and pipeline orchestration
# --------------------

class VoiceGenRequest(BaseModel):
    registry: Optional[str] = Field(default="single")
    category: Optional[str] = Field(default="general")
    voice: Optional[str] = Field(default="en-US-Neural-1")
    limit: Optional[int] = Field(default=12, ge=1, le=200)


@APP.post("/api/agents/voice/generate")
def generate_voice(payload: VoiceGenRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    try:
        out = run_voice(
            registry=payload.registry or "single",
            category=payload.category or "general",
            voice=payload.voice or "en-US-Neural-1",
            limit=payload.limit or 12,
        )
        return {"status": "ok", "meta": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "voice_generation_failed", "message": str(e)})


class AvatarRenderRequest(BaseModel):
    registry: Optional[str] = Field(default="single")
    category: Optional[str] = Field(default="general")
    style: Optional[str] = Field(default="news-anchor")


@APP.post("/api/agents/avatar/render")
def render_avatar(payload: AvatarRenderRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    try:
        out = run_avatar(registry=payload.registry or "single", category=payload.category or "general", style=payload.style or "news-anchor")
        return {"status": "ok", "meta": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "avatar_render_failed", "message": str(e)})

class VoiceItemRequest(BaseModel):
    title: str
    narration: str
    lang: Optional[str] = Field(default="en")
    accent: Optional[str] = Field(default=None)  # e.g., en-US, en-IN, hi-IN
    emotion: Optional[int] = Field(default=40, ge=0, le=100)  # 0–100 depth
    audience: Optional[str] = Field(default="general")
    tone: Optional[str] = Field(default="news")
    voice: Optional[str] = Field(default="en-US-Neural-1")
    category: Optional[str] = Field(default="general")

@APP.post("/api/agents/voice/generate_item")
def generate_voice_item(payload: VoiceItemRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        lang = (payload.lang or "en").lower()
        # Map accent to language group where applicable
        if payload.accent:
            a = payload.accent.lower()
            if a.startswith("hi"):
                lang = "hi"
            elif a.startswith("ta"):
                lang = "ta"
            elif a.startswith("bn"):
                lang = "bn"
            elif a.startswith("en"):
                lang = "en"
        script = {
            "title": payload.title,
            "lang": lang,
            "audience": payload.audience or "general",
            "tone": payload.tone or "news",
            "variants": {
                "narration": payload.narration,
            },
            "metadata": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "category": payload.category or "general",
            },
        }
        agent = TTSAgentStub(voice=payload.voice or "en-US-Neural-1")
        try:
            if isinstance(payload.emotion, int):
                agent.rate = int(max(90, min(220, 120 + (payload.emotion or 0))))
        except Exception:
            pass
        out = agent.synthesize([script], category=payload.category or "general")
        item = out[0] if out else {}
        return {"status": "ok", "item": item}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "voice_item_failed", "message": str(e)})

class AvatarItemRequest(BaseModel):
    title: str
    narration: Optional[str] = None
    audio_url: Optional[str] = None
    lang: Optional[str] = Field(default="en")
    accent: Optional[str] = Field(default=None)
    emotion: Optional[int] = Field(default=40, ge=0, le=100)
    voice: Optional[str] = Field(default="en-US-Neural-1")
    style: Optional[str] = Field(default="news-anchor")
    category: Optional[str] = Field(default="general")

@APP.post("/api/agents/avatar/render_item")
def render_avatar_item(payload: AvatarItemRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        voice_item: Dict[str, Any] = {}
        lang = (payload.lang or "en").lower()
        if payload.accent:
            a = payload.accent.lower()
            if a.startswith("hi"):
                lang = "hi"
            elif a.startswith("ta"):
                lang = "ta"
            elif a.startswith("bn"):
                lang = "bn"
            elif a.startswith("en"):
                lang = "en"
        if payload.audio_url:
            fname = os.path.basename(payload.audio_url)
            tts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data", "tts"))
            audio_path = os.path.join(tts_dir, fname)
            voice_item = {
                "title": payload.title,
                "lang": lang,
                "voice": payload.voice or "en-US-Neural-1",
                "audio_url": f"/data/tts/{fname}",
                "audio_path": audio_path,
            }
        else:
            if not payload.narration:
                raise HTTPException(status_code=400, detail={"error": "missing_narration_or_audio", "message": "Either 'audio_url' or 'narration' is required"})
            s = {
                "title": payload.title,
                "lang": lang,
                "audience": "general",
                "tone": "news",
                "variants": {"narration": payload.narration},
            }
            tts = TTSAgentStub(voice=payload.voice or "en-US-Neural-1")
            try:
                if isinstance(payload.emotion, int):
                    tts.rate = int(max(90, min(220, 120 + (payload.emotion or 0))))
            except Exception:
                pass
            out = tts.synthesize([s], category=payload.category or "general")
            voice_item = out[0] if out else {}
        agent = AvatarAgentStub(style=payload.style or "news-anchor")
        vids = agent.render([voice_item], category=payload.category or "general")
        item = vids[0] if vids else {}
        return {"status": "ok", "item": item}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "avatar_item_failed", "message": str(e)})


class PipelineRunRequest(BaseModel):
    registry: Optional[str] = Field(default="single")
    category: Optional[str] = Field(default="general")
    voice: Optional[str] = Field(default="en-US-Neural-1")
    style: Optional[str] = Field(default="news-anchor")


@APP.post("/api/pipeline/run")
def run_pipeline(payload: PipelineRunRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    # Lightweight chaining: fetch -> filter -> scripts -> voice -> avatar
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    try:
        sf = run_fetch(registry=payload.registry or "single", category=payload.category or "general")
        fl = run_filter(registry=payload.registry or "single", category=payload.category or "general")
        sc = run_scripts(registry=payload.registry or "single", category=payload.category or "general")
        vc = run_voice(registry=payload.registry or "single", category=payload.category or "general", voice=payload.voice or "en-US-Neural-1")
        av = run_avatar(registry=payload.registry or "single", category=payload.category or "general", style=payload.style or "news-anchor")
        return {
            "status": "ok",
            "stages": {
                "fetch": sf,
                "filter": fl,
                "scripts": sc,
                "voice": vc,
                "avatar": av,
            },
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "pipeline_run_failed", "message": str(e)})


# --------------------
# Dashboard stats endpoint
# --------------------

DASH_RATE_LIMIT_PER_MINUTE = 120
_dash_rate_buckets: Dict[str, Dict[str, Any]] = {}


_dash_cache: Dict[str, Dict[str, Any]] = {}
DASH_CACHE_MAX_ENTRIES = int(os.getenv("DASH_CACHE_MAX_ENTRIES", "500"))


def _get_dash_cache(key: str) -> Optional[Dict[str, Any]]:
    entry = _dash_cache.get(key)
    if not entry:
        return None
    if time.time() > entry["expires"]:
        _dash_cache.pop(key, None)
        return None
    return entry["data"]


def _set_dash_cache(key: str, data: Dict[str, Any]) -> None:
    # 30 seconds TTL
    now = time.time()
    # Cleanup expired entries
    expired = [k for k, v in _dash_cache.items() if now > v.get("expires", 0)]
    for k in expired:
        _dash_cache.pop(k, None)
    # Enforce size limit
    if len(_dash_cache) >= DASH_CACHE_MAX_ENTRIES:
        oldest = sorted(_dash_cache.items(), key=lambda kv: kv[1].get("expires", now))[: max(1, DASH_CACHE_MAX_ENTRIES // 10)]
        for k, _ in oldest:
            _dash_cache.pop(k, None)
    _dash_cache[key] = {"expires": now + 30, "data": data}


def _secs_for_range(r: str) -> int:
    r = (r or "24h").lower()
    return {
        "24h": 24 * 3600,
        "7d": 7 * 24 * 3600,
        "30d": 30 * 24 * 3600,
    }.get(r, 24 * 3600)


def _output_root() -> str:
    root = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output")
    return os.path.abspath(root)


def _files_modified_within(pattern: str, secs: int) -> List[str]:
    root = _output_root()
    paths = glob.glob(os.path.join(root, pattern))
    cutoff = time.time() - secs
    return [p for p in paths if os.path.getmtime(p) >= cutoff]


def _count_items_in_files(paths: List[str]) -> int:
    total = 0
    for p in paths:
        try:
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                total += len(data)
        except Exception:
            continue
    return total


def _ago_str(seconds: float) -> str:
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds//60)}m ago"
    if seconds < 86400:
        return f"{int(seconds//3600)}h ago"
    return f"{int(seconds//86400)}d ago"


@APP.get("/api/dashboard/stats")
def get_dashboard_stats(time_range: Optional[str] = "24h", response: Response = None, auth: AuthContext = Depends(require_auth)):
    # Rate limit per user
    err, info = _apply_rate_limit(_dash_rate_buckets, DASH_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    key = f"stats|{time_range}"
    cached = _get_dash_cache(key)
    if cached is not None:
        return cached

    secs = _secs_for_range(time_range or "24h")
    now = time.time()
    root = _output_root()

    # Metrics
    items_files = glob.glob(os.path.join(root, "*_items.json"))
    filtered_files = _files_modified_within("*_filtered.json", secs)
    scripts_files = _files_modified_within("*_scripts.json", secs)
    voice_files = _files_modified_within("*_voice.json", secs)
    avatar_files = _files_modified_within("*_avatar.json", secs)
    items_recent = _files_modified_within("*_items.json", secs)

    stage_activity = {
        "items": bool(items_recent),
        "filtered": bool(filtered_files),
        "scripts": bool(scripts_files),
        "voice": bool(voice_files),
        "avatar": bool(avatar_files),
    }
    active_ai_agents = sum(1 for v in stage_activity.values() if v)
    total_ai_agents = 5

    content_generated = _count_items_in_files(scripts_files) or _count_items_in_files(items_recent)

    # Recent content list from items (fallback to scripts titles if needed)
    recent_content = []
    try:
        items = _load_items_from_output(None)
        for i in items[:5]:
            # derive relative time from published_at
            try:
                ts_iso = i["metadata"]["published_at"]
                dt = datetime.fromisoformat(ts_iso).timestamp()
                ts_rel = _ago_str(max(0.0, now - dt))
            except Exception:
                ts_rel = "recent"
            recent_content.append({
                "title": i.get("title", "Untitled"),
                "generated_by": "Summarizer-Pro" if i.get("metadata", {}).get("category") != "science" else "FactChecker-Alpha",
                "timestamp": ts_rel,
                "category": i.get("metadata", {}).get("category", "general"),
            })
    except Exception as e:
        log.warning("recent_content_build_failed")

    response = {
        "system_status": {
            "status": "operational",
            "message": "All services running smoothly",
            "last_check": datetime.now(timezone.utc).isoformat(),
        },
        "metrics": {
            "live_news_feeds": len(items_files),
            "active_ai_agents": active_ai_agents,
            "total_ai_agents": total_ai_agents,
            "content_generated_24h": content_generated,
        },
        "recent_content": recent_content,
    }

    _set_dash_cache(key, response)
    return response


# --------------------
# User preferences endpoint
# --------------------

def _data_root() -> str:
    root = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data")
    return os.path.abspath(root)


def _read_user_preferences(user_id: str) -> Dict[str, Any]:
    # Prefer DB-backed preferences if available
    try:
        row = db_get_user_prefs(user_id)
        if row:
            return {
                "user_id": user_id,
                "preferences": {
                    "language": row.get("language"),
                    "region": row.get("region"),
                    "theme": row.get("theme"),
                    "preferred_categories": row.get("preferred_categories") or [],
                    "notification_preferences": row.get("notification_preferences") or {},
                },
                "last_updated": row.get("updated_at"),
            }
    except Exception as e:
        log.warning("db_get_user_prefs_failed")

    # Try file-backed preferences: single_pipeline/data/preferences_{user_id}.json
    path = _safe_join(_data_root(), f"preferences_{_sanitize_identifier(user_id)}.json")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                prefs = json.load(f)
            last_updated = datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).isoformat()
            return {
                "user_id": user_id,
                "preferences": prefs,
                "last_updated": last_updated,
            }
    except Exception as e:
        log.warning("file_prefs_read_failed")

    # Defaults
    prefs = {
        "language": "English",
        "region": "Global",
        "theme": "Dark",
        "preferred_categories": ["Finance", "Tech"],
        "excluded_categories": [],
        "notification_preferences": {
            "email_notifications": True,
            "in_app_notifications": True,
            "desktop_alerts": False,
        },
    }
    return {
        "user_id": user_id,
        "preferences": prefs,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


PREF_GET_RATE_LIMIT_PER_MINUTE = 30
_pref_get_rate_buckets: Dict[str, Dict[str, Any]] = {}


@APP.get("/api/users/{user_id}/preferences")
def get_user_preferences(user_id: str, response: Response, auth: AuthContext = Depends(require_auth)):
    # Auth: user can read own preferences unless admin
    if auth.user_id != user_id and (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    err, info = _apply_rate_limit(_pref_get_rate_buckets, PREF_GET_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    # No caching; load on request
    return _read_user_preferences(user_id)


class NotificationPreferences(BaseModel):
    email_notifications: Optional[bool] = None
    in_app_notifications: Optional[bool] = None
    desktop_alerts: Optional[bool] = None


class PreferencesUpdate(BaseModel):
    language: Optional[str] = None
    region: Optional[str] = None
    theme: Optional[str] = None
    notification_preferences: Optional[NotificationPreferences] = None


def _write_user_preferences(user_id: str, updates: PreferencesUpdate) -> Dict[str, Any]:
    current = _read_user_preferences(user_id)
    prefs = current.get("preferences", {})

    if updates.language is not None:
        prefs["language"] = updates.language
    if updates.region is not None:
        prefs["region"] = updates.region
    if updates.theme is not None:
        prefs["theme"] = updates.theme
    if updates.notification_preferences is not None:
        # Merge nested notification prefs, allowing partial updates
        existing_np = prefs.get("notification_preferences", {
            "email_notifications": True,
            "in_app_notifications": True,
            "desktop_alerts": False,
        })
        update_np = updates.notification_preferences.dict(exclude_none=True)
        existing_np.update(update_np)
        prefs["notification_preferences"] = existing_np

    # Persist to DB first (best-effort)
    try:
        db_upsert_user_prefs(user_id, prefs)
    except Exception as e:
        log.warning("db_upsert_user_prefs_failed")

    # Persist to file with atomic replace
    os.makedirs(_data_root(), exist_ok=True)
    path = _safe_join(_data_root(), f"preferences_{_sanitize_identifier(user_id)}.json")
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(prefs, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass

    updated_at = datetime.now(timezone.utc).isoformat()
    return {"success": True, "message": "Preferences updated successfully", "updated_at": updated_at}


PREF_PUT_RATE_LIMIT_PER_MINUTE = 10
_pref_put_rate_buckets: Dict[str, Dict[str, Any]] = {}


@APP.put("/api/users/{user_id}/preferences")
def update_user_preferences(user_id: str, payload: PreferencesUpdate, response: Response, auth: AuthContext = Depends(require_auth)):
    # Auth: user can update own preferences unless admin
    if auth.user_id != user_id and (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    err, info = _apply_rate_limit(_pref_put_rate_buckets, PREF_PUT_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        return _write_user_preferences(user_id, payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --------------------
# Trending articles endpoint
# --------------------

TREND_RATE_LIMIT_PER_MINUTE = 60
_trend_rate_buckets: Dict[str, Dict[str, Any]] = {}


_trend_cache: Dict[str, Dict[str, Any]] = {}
TREND_CACHE_MAX_ENTRIES = int(os.getenv("TREND_CACHE_MAX_ENTRIES", "500"))


def _get_trend_cache(key: str) -> Optional[Dict[str, Any]]:
    entry = _trend_cache.get(key)
    if not entry:
        return None
    if time.time() > entry["expires"]:
        _trend_cache.pop(key, None)
        return None
    return entry["data"]


def _set_trend_cache(key: str, data: Dict[str, Any]) -> None:
    # 2 minutes TTL
    now = time.time()
    # Cleanup expired entries
    expired = [k for k, v in _trend_cache.items() if now > v.get("expires", 0)]
    for k in expired:
        _trend_cache.pop(k, None)
    # Enforce size limit
    if len(_trend_cache) >= TREND_CACHE_MAX_ENTRIES:
        oldest = sorted(_trend_cache.items(), key=lambda kv: kv[1].get("expires", now))[: max(1, TREND_CACHE_MAX_ENTRIES // 10)]
        for k, _ in oldest:
            _trend_cache.pop(k, None)
    _trend_cache[key] = {"expires": now + 120, "data": data}


def _secs_for_timeframe(tf: str) -> int:
    tf = (tf or "24h").lower()
    return {
        "1h": 3600,
        "24h": 24 * 3600,
        "7d": 7 * 24 * 3600,
    }.get(tf, 24 * 3600)


def _epoch_from_iso(ts: Optional[str]) -> float:
    try:
        if not ts:
            return time.time()
        return datetime.fromisoformat(ts).timestamp()
    except Exception:
        return time.time()


def _collect_trending(secs: int, category: Optional[str]) -> List[Dict[str, Any]]:
    """Aggregate engagement and shares per dedup group using DB-backed articles.

    Group key is `group_key` if present; otherwise the article id acts as its own group.
    Representative selection policy: latest within timeframe (or latest overall if none).
    """
    now = time.time()
    views_by: Dict[str, int] = {}
    engaged_by: Dict[str, int] = {}
    quality_sum_by: Dict[str, float] = {}
    shares_by: Dict[str, int] = {}

    # Aggregate engagement events within timeframe
    for ev in _engagement_events:
        ts = _epoch_from_iso(ev.get("timestamp"))
        if now - ts > secs:
            continue
        aid = ev.get("article_id")
        if not aid:
            continue
        row = db_get_article_by_id(aid)
        if not row:
            continue
        art_cat = (row.get("category") or "general").lower()
        if category and art_cat != category.lower():
            continue
        gid = row.get("group_key") or aid
        views_by[gid] = views_by.get(gid, 0) + 1
        q = float(ev.get("quality_score", 0.0))
        quality_sum_by[gid] = quality_sum_by.get(gid, 0.0) + q
        if q >= 0.6:
            engaged_by[gid] = engaged_by.get(gid, 0) + 1

    # Aggregate shares within timeframe
    for fb in _feedback_events:
        ts = _epoch_from_iso(fb.get("timestamp"))
        if now - ts > secs:
            continue
        if fb.get("action") != "share":
            continue
        aid = fb.get("article_id")
        if not aid:
            continue
        row = db_get_article_by_id(aid)
        if not row:
            continue
        art_cat = (row.get("category") or "general").lower()
        if category and art_cat != category.lower():
            continue
        gid = row.get("group_key") or aid
        shares_by[gid] = shares_by.get(gid, 0) + 1

    # Union of groups seen in timeframe
    all_gids = set(views_by.keys()) | set(shares_by.keys())

    # Build metrics list per group
    items: List[Dict[str, Any]] = []
    max_views = max(views_by.values()) if views_by else 0
    max_shares = max(shares_by.values()) if shares_by else 0

    for gid in all_gids:
        # Choose representative article: latest within timeframe if possible
        rep = db_get_group_representative(gid if gid else "", secs=secs, category=category)
        if not rep:
            # Fallback to latest overall within group
            rep = db_get_group_representative(gid if gid else "", secs=None, category=category)
        if not rep:
            continue
        title = rep.get("title") or "Untitled"
        views = views_by.get(gid, 0)
        shares = shares_by.get(gid, 0)
        engaged = engaged_by.get(gid, 0)
        engagement_rate = (engaged / views) if views > 0 else 0.0
        v_norm = (views / max_views) if max_views > 0 else 0.0
        s_norm = (shares / max_shares) if max_shares > 0 else 0.0
        trending_score = max(0.0, min(1.0, 0.5 * engagement_rate + 0.3 * v_norm + 0.2 * s_norm))
        items.append({
            "group_key": rep.get("group_key") or rep.get("id"),
            "id": rep.get("id"),
            "title": title,
            "trending_score": round(trending_score, 4),
            "engagement_rate": round(engagement_rate, 4),
            "views": views,
            "shares": shares,
        })

    # Sort by trending score desc
    items.sort(key=lambda x: x["trending_score"], reverse=True)
    return items


@APP.get("/api/articles/trending")
def get_trending_articles(timeframe: Optional[str] = "24h", category: Optional[str] = None, limit: int = 20, response: Response = None, auth: AuthContext = Depends(require_auth)):
    # Per-user rate limit
    err, info = _apply_rate_limit(_trend_rate_buckets, TREND_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    # Validate
    if limit <= 0 or limit > 100:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 100")
    allowed = {"1h", "24h", "7d"}
    if timeframe not in allowed:
        timeframe = "24h"
    # Category validation (simple supported set)
    if category:
        allowed_categories = {"general", "finance", "tech", "science"}
        if category.lower() not in allowed_categories:
            return _error("invalid_category", 400, "Category not supported", details={"category": category})

    # Cache lookup
    key = f"trending|{timeframe}|{category or ''}|{limit}"
    cached = _get_trend_cache(key)
    if cached is not None:
        return cached

    secs = _secs_for_timeframe(timeframe)
    items = _collect_trending(secs, category)
    total = len(items)
    items = items[:limit]

    response = {
        "articles": items,
        "meta": {
            "timeframe": timeframe,
            "total_trending": total,
        },
    }

    _set_trend_cache(key, response)
    return response


# --------------------
# Debug graph endpoint
# --------------------

@APP.post("/api/debug/graph/build")
def build_debug_graph(response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    try:
        out = build_graph_from_traces()
        return {"status": "ok", "graph": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "graph_build_failed", "message": str(e)})


# --------------------
# UI-friendly wrapper endpoints for Day 1
# --------------------

class BasicPipelineRequest(BaseModel):
    registry: Optional[str] = Field(default="single")
    category: Optional[str] = Field(default="general")
    limit_preview: Optional[int] = Field(default=10, ge=1, le=50)


def _read_json_list(path: str) -> List[Dict[str, Any]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else (data.get("items") or [])
    except Exception:
        return []


@APP.post("/fetch")
def ui_fetch(payload: BasicPipelineRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        registry = payload.registry or "single"
        category = payload.category or "general"
        out = run_fetch(registry=registry, category=category)
        base = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output")
        items_path = _safe_join(os.path.abspath(base), f"{_sanitize_identifier(registry)}_items.json")
        items = _read_json_list(items_path)
        if not isinstance(items, list):
            items = []
        if len(items) == 0:
            seed = [{
                "title": "Live pipeline seed",
                "body": "Seeded item to keep stages active until feeds populate.",
                "timestamp": int(time.time()),
                "category": category,
            }]
            try:
                with open(items_path, "w", encoding="utf-8") as f:
                    json.dump(seed, f, ensure_ascii=False, indent=2)
                items = seed
            except Exception:
                items = []
        preview = items[: int(payload.limit_preview or 10)]
        return {"status": "ok", "count": len(items), "preview": preview, "files": {"items": items_path}}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "fetch_failed", "message": str(e)})


@APP.post("/process")
def ui_process(payload: BasicPipelineRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        registry = payload.registry or "single"
        category = payload.category or "general"
        fl = run_filter(registry=registry, category=category)
        sc = run_scripts(registry=registry, category=category)
        base = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output")
        filtered_path = _safe_join(os.path.abspath(base), f"{_sanitize_identifier(registry)}_filtered.json")
        scripts_path = _safe_join(os.path.abspath(base), f"{_sanitize_identifier(registry)}_scripts.json")
        filtered = _read_json_list(filtered_path)
        scripts = _read_json_list(scripts_path)
        # UI preview aligns with Noopur’s schema-like payload
        preview = [
            {
                "title": s.get("title"),
                "lang": s.get("lang"),
                "audience": s.get("audience"),
                "tone": s.get("tone"),
                "variants": s.get("variants"),
                "metadata": s.get("metadata"),
            }
            for s in scripts[: int(payload.limit_preview or 10)]
        ]
        return {
            "status": "ok",
            "counts": {"filtered": len(filtered), "scripts": len(scripts)},
            "preview": preview,
            "files": {"filtered": filtered_path, "scripts": scripts_path},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "process_failed", "message": str(e)})


@APP.post("/voice")
def ui_voice(payload: VoiceGenRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    err, info = _apply_rate_limit(_rate_buckets, RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        registry = payload.registry or "single"
        category = payload.category or "general"
        voice_opt = payload.voice or "en-US-Neural-1"
        out = run_voice(registry=registry, category=category, voice=voice_opt, limit=payload.limit or 12)
        base = os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "output")
        voice_path = _safe_join(os.path.abspath(base), f"{_sanitize_identifier(registry)}_voice.json")
        voice_items = _read_json_list(voice_path)
        preview = voice_items[:10]
        return {"status": "ok", "count": len(voice_items), "preview": preview, "files": {"voice": voice_path}}
    except Exception as e:
        raise HTTPException(status_code=500, detail={"error": "voice_failed", "message": str(e)})


@APP.post("/feedback")
def ui_feedback(payload: ArticleFeedbackRequest, response: Response, auth: AuthContext = Depends(require_auth)):
    # Delegate to the existing feedback endpoint logic for consistency
    return submit_article_feedback(payload, response, auth)

# --------------------
# Static UI mounting
# --------------------
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if not os.path.isdir(STATIC_DIR):
    try:
        os.makedirs(STATIC_DIR, exist_ok=True)
    except Exception:
        pass

APP.mount("/ui", StaticFiles(directory=STATIC_DIR, html=True), name="ui")

# Serve generated media (TTS WAVs, Avatar JSON) for UI preview/playback
_DATA_TTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data", "tts"))
_DATA_AVATAR_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data", "avatar"))
try:
    os.makedirs(_DATA_TTS_DIR, exist_ok=True)
    os.makedirs(_DATA_AVATAR_DIR, exist_ok=True)
except Exception:
    pass

APP.mount("/data/tts", StaticFiles(directory=_DATA_TTS_DIR), name="data_tts")
APP.mount("/data/avatar", StaticFiles(directory=_DATA_AVATAR_DIR), name="data_avatar")


@APP.get("/")
def root_redirect():
    return RedirectResponse(url="/ui/")
@APP.on_event("startup")
def _startup():
    try:
        init_db()
    except Exception as e:
        log.warning("db_init_failed")

# --------------------
# Admin: Feeds Registry (JWT admin only)
# --------------------

class Feed(BaseModel):
    id: str
    type: str
    cadence_seconds: int
    channel: Optional[str] = None  # telegram
    handle: Optional[str] = None   # x
    channel_id: Optional[str] = None  # youtube_rss


class RegistryUpload(BaseModel):
    # Accept raw dicts to preserve unknown fields for validation warnings
    feeds: Optional[List[Dict[str, Any]]] = None
    yaml: Optional[str] = None


ADMIN_FEEDS_RATE_LIMIT_PER_MINUTE = 10
_admin_feeds_get_rate_buckets: Dict[str, Dict[str, Any]] = {}
_admin_feeds_post_rate_buckets: Dict[str, Dict[str, Any]] = {}
_admin_feeds_reload_rate_buckets: Dict[str, Dict[str, Any]] = {}


@APP.get("/api/admin/feeds/registry")
def get_feeds_registry(response: Response, auth: AuthContext = Depends(require_auth)):
    if (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    err, info = _apply_rate_limit(_admin_feeds_get_rate_buckets, ADMIN_FEEDS_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        reg = load_registry(DEFAULT_REGISTRY_PATH)
        return {"path": DEFAULT_REGISTRY_PATH, "registry": reg}
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@APP.post("/api/admin/feeds/registry")
def post_feeds_registry(payload: RegistryUpload, response: Response, auth: AuthContext = Depends(require_auth)):
    if (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    err, info = _apply_rate_limit(_admin_feeds_post_rate_buckets, ADMIN_FEEDS_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])

    feeds_data: List[Dict[str, Any]] = []
    if payload.yaml:
        # Parse YAML directly
        try:
            reg = load_registry(path=None)  # ensure yaml dependency check
            import yaml  # type: ignore
            parsed = yaml.safe_load(payload.yaml) or {}
            feeds_data = (parsed.get("feeds") or [])
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"invalid_yaml: {e}")
    elif payload.feeds:
        # Preserve as-is to allow unknown fields to be surfaced in warnings
        feeds_data = payload.feeds or []
    else:
        raise HTTPException(status_code=400, detail="either 'yaml' string or 'feeds' list is required")

    # Validate with warnings, save YAML canonicalized
    try:
        validated, warnings = validate_feeds(feeds_data)
        path = save_registry_yaml(validated, DEFAULT_REGISTRY_PATH)
        return {"result": "ok", "path": path, "feeds": len(validated), "warnings": warnings}
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@APP.post("/api/admin/feeds/reload")
def post_feeds_reload(registry_name: Optional[str] = "single", response: Response = None, auth: AuthContext = Depends(require_auth)):
    if (auth.role or "user") != "admin":
        raise HTTPException(status_code=403, detail="forbidden")
    err, info = _apply_rate_limit(_admin_feeds_reload_rate_buckets, ADMIN_FEEDS_RATE_LIMIT_PER_MINUTE, auth.user_id)
    if err is not None:
        return err
    response.headers["X-RateLimit-Limit"] = str(info["limit"])
    response.headers["X-RateLimit-Remaining"] = str(info["remaining"])
    response.headers["X-RateLimit-Reset"] = str(info["reset"])
    try:
        summary = hot_reload(registry_name=registry_name, path=DEFAULT_REGISTRY_PATH)
        return summary
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
