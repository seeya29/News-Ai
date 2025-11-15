import os
import glob
import json
import time
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Depends, Response
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from pydantic import BaseModel, Field
import base64
from db import (
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
)


APP = FastAPI(title="News-Ai API", version="0.1.0")

# --------------------
# Authentication (JWT, lightweight decode)
# --------------------
security = HTTPBearer(auto_error=True)


class AuthContext(BaseModel):
    user_id: str
    role: Optional[str] = "user"
    exp: int


def _decode_jwt_payload(token: str) -> Dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return {}
        payload_b64 = parts[1]
        padding = "=" * (-len(payload_b64) % 4)
        raw = base64.urlsafe_b64decode(payload_b64 + padding)
        return json.loads(raw)
    except Exception:
        return {}


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(security)) -> AuthContext:
    claims = _decode_jwt_payload(credentials.credentials)
    user_id = claims.get("user_id")
    role = claims.get("role") or "user"
    exp = claims.get("exp")
    if not user_id or not exp:
        raise HTTPException(status_code=401, detail="invalid_token")
    if time.time() > float(exp):
        raise HTTPException(status_code=401, detail="token_expired")
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


def _check_rate_limit(user_id: str) -> Optional[JSONResponse]:
    now = int(time.time())
    bucket = _rate_buckets.get(user_id)
    if not bucket:
        _rate_buckets[user_id] = {"window": now // 60, "count": 1}
        return None
    window = now // 60
    if bucket["window"] != window:
        _rate_buckets[user_id] = {"window": window, "count": 1}
        return None
    if bucket["count"] >= RATE_LIMIT_PER_MINUTE:
        return JSONResponse(status_code=429, content={
            "error": "rate_limit_exceeded",
            "detail": f"Max {RATE_LIMIT_PER_MINUTE} requests per minute",
        })
    bucket["count"] += 1
    return None


# 5-minute TTL cache per user+params
CACHE_TTL_SECONDS = 300
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
    _feed_cache[key] = {"expires": time.time() + CACHE_TTL_SECONDS, "data": data}


def _iso(dt: Optional[float]) -> str:
    if dt is None:
        return datetime.now(timezone.utc).isoformat()
    try:
        # Assume seconds since epoch
        return datetime.fromtimestamp(float(dt), tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


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
                    # build response shape
                    items.append({
                        "id": _hash_id(title, body),
                        "title": title or "Untitled",
                        "source": {
                            "name": "News-Ai",
                            "logo_url": "https://cdn.newsai.com/sources/news-ai.png",
                        },
                        "metadata": {
                            "published_at": _iso(ts),
                            "category": item_cat,
                            "reading_time_minutes": _reading_time_minutes(body),
                        },
                        "relevance_score": 0.75,
                        "thumbnail_url": "https://cdn.newsai.com/thumbs/default.jpg",
                        "processing_status": "verifying",
                        "processing_progress": 50,
                        "processing_stage": "Verify",
                    })
        except Exception:
            # Skip bad files silently for now
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
            "logo_url": "https://cdn.newsai.com/sources/news-ai.png",
        },
        "metadata": {
            "published_at": row.get("published_at") or datetime.now(timezone.utc).isoformat(),
            "category": (row.get("category") or "general").lower(),
            "reading_time_minutes": 3,
        },
        "relevance_score": float(row.get("relevance_score") or 0.75),
        "thumbnail_url": row.get("thumbnail_url") or "https://cdn.newsai.com/thumbs/default.jpg",
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
        return JSONResponse(status_code=429, content={
            "error": "rate_limit_exceeded",
            "detail": f"Max {FEEDBACK_RATE_LIMIT_PER_MINUTE} requests per minute",
        })
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

    # Find article
    article = _find_article_by_id(payload.article_id)
    if not article:
        return _error("article_not_found", 404, f"Article with ID '{payload.article_id}' does not exist")

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
    # Persist to DB (best-effort)
    try:
        db_insert_feedback(event)
    except Exception:
        pass

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
        return JSONResponse(status_code=429, content={
            "error": "rate_limit_exceeded",
            "detail": f"Max {ENG_RATE_LIMIT_PER_MINUTE} requests per minute",
        })
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
            return {"success": True, "engagement_id": eid, "quality_score": q, "message": "Engagement metrics recorded"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# --------------------
# Dashboard stats endpoint
# --------------------

DASH_RATE_LIMIT_PER_MINUTE = 120
_dash_rate_buckets: Dict[str, Dict[str, Any]] = {}


_dash_cache: Dict[str, Dict[str, Any]] = {}


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
    _dash_cache[key] = {"expires": time.time() + 30, "data": data}


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
    except Exception:
        pass

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
    except Exception:
        # Fall through to file-backed
        pass

    # Try file-backed preferences: single_pipeline/data/preferences_{user_id}.json
    path = os.path.join(_data_root(), f"preferences_{user_id}.json")
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
    except Exception:
        # Fall through to defaults
        pass

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
    except Exception:
        # continue with file persistence as fallback
        pass

    # Persist to file with atomic replace
    os.makedirs(_data_root(), exist_ok=True)
    path = os.path.join(_data_root(), f"preferences_{user_id}.json")
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
    _trend_cache[key] = {"expires": time.time() + 120, "data": data}


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
@APP.on_event("startup")
def _startup():
    try:
        init_db()
    except Exception:
        # DB init is best-effort; file-backed paths continue to work
        pass