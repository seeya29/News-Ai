import os
import sqlite3
import json
from typing import Any, Dict, Optional, List
from datetime import datetime, timezone

_DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data", "app.db"))
_CONN: Optional[sqlite3.Connection] = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _get_conn() -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        _CONN = _connect()
    return _CONN


def init_db() -> None:
    conn = _get_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY,
            title TEXT,
            source_name TEXT,
            source_url TEXT,
            thumbnail_url TEXT,
            category TEXT,
            published_at TEXT,
            relevance_score REAL,
            processing_status TEXT,
            processing_progress INTEGER,
            group_key TEXT,
            created_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_articles_cat_pub ON articles(category, published_at DESC);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_source_url ON articles(source_url);
        CREATE INDEX IF NOT EXISTS idx_articles_groupkey ON articles(group_key);

        CREATE TABLE IF NOT EXISTS user_feedback (
            id TEXT PRIMARY KEY,
            user_id TEXT,
            article_id TEXT,
            action TEXT,
            timestamp TEXT,
            context TEXT,
            created_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_feedback_user_time ON user_feedback(user_id, timestamp);
        CREATE INDEX IF NOT EXISTS idx_feedback_article_time ON user_feedback(article_id, timestamp);

        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id TEXT PRIMARY KEY,
            language TEXT,
            region TEXT,
            theme TEXT,
            preferred_categories TEXT,
            notification_preferences TEXT,
            updated_at TEXT
        );

        -- Pipeline visibility tables
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            source TEXT,
            category TEXT,
            status TEXT, -- running|completed|failed
            started_at TEXT,
            ended_at TEXT,
            meta TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_runs_started ON pipeline_runs(started_at);
        CREATE INDEX IF NOT EXISTS idx_runs_status ON pipeline_runs(status);

        CREATE TABLE IF NOT EXISTS pipeline_stage_events (
            run_id TEXT,
            stage TEXT, -- fetch|filter|dedup|summarize|avatar|voice|publish
            status TEXT, -- running|completed|failed
            progress INTEGER,
            error_code TEXT,
            error_message TEXT,
            started_at TEXT,
            ended_at TEXT,
            duration_ms INTEGER,
            meta TEXT,
            PRIMARY KEY (run_id, stage)
        );
        CREATE INDEX IF NOT EXISTS idx_stage_started ON pipeline_stage_events(started_at);
        CREATE INDEX IF NOT EXISTS idx_stage_status ON pipeline_stage_events(status);
        """
    )
    # Backfill migration: add group_key if the column was missing in existing DBs
    try:
        conn.execute("ALTER TABLE articles ADD COLUMN group_key TEXT")
    except Exception:
        pass
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_groupkey ON articles(group_key)")
    except Exception:
        pass
    conn.commit()

# --------------------
# Pipeline visibility helpers
# --------------------

def upsert_pipeline_run(run: Dict[str, Any]) -> None:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, source, category, status, started_at, ended_at, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            source=excluded.source,
            category=excluded.category,
            status=excluded.status,
            started_at=COALESCE(excluded.started_at, pipeline_runs.started_at),
            ended_at=excluded.ended_at,
            meta=excluded.meta
        """,
        (
            run.get("run_id"),
            run.get("source"),
            run.get("category"),
            run.get("status"),
            run.get("started_at"),
            run.get("ended_at"),
            json.dumps(run.get("meta") or {}),
        ),
    )
    conn.commit()


def upsert_stage_event(ev: Dict[str, Any]) -> None:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO pipeline_stage_events (run_id, stage, status, progress, error_code, error_message, started_at, ended_at, duration_ms, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, stage) DO UPDATE SET
            status=excluded.status,
            progress=COALESCE(excluded.progress, pipeline_stage_events.progress),
            error_code=excluded.error_code,
            error_message=excluded.error_message,
            started_at=COALESCE(excluded.started_at, pipeline_stage_events.started_at),
            ended_at=excluded.ended_at,
            duration_ms=excluded.duration_ms,
            meta=excluded.meta
        """,
        (
            ev.get("run_id"),
            ev.get("stage"),
            ev.get("status"),
            int(ev.get("progress")) if ev.get("progress") is not None else None,
            ev.get("error_code"),
            ev.get("error_message"),
            ev.get("started_at"),
            ev.get("ended_at"),
            int(ev.get("duration_ms")) if ev.get("duration_ms") is not None else None,
            json.dumps(ev.get("meta") or {}),
        ),
    )
    conn.commit()


def get_runs_in_timeframe(secs: int) -> List[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.execute(
        """
        SELECT * FROM pipeline_runs
        WHERE datetime(started_at) >= datetime('now', ? || ' seconds')
        ORDER BY datetime(started_at) DESC
        """,
        (f'-{int(secs)}',),
    )
    rows = cur.fetchall()
    return [{k: r[k] for k in r.keys()} for r in rows]


def get_stage_events_for_runs(run_ids: List[str]) -> List[Dict[str, Any]]:
    if not run_ids:
        return []
    conn = _get_conn()
    placeholders = ",".join(["?"] * len(run_ids))
    sql = f"SELECT * FROM pipeline_stage_events WHERE run_id IN ({placeholders})"
    cur = conn.execute(sql, tuple(run_ids))
    rows = cur.fetchall()
    return [{k: r[k] for k in r.keys()} for r in rows]


def get_user_preferences(user_id: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.execute("SELECT * FROM user_preferences WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if not row:
        return None
    try:
        preferred_categories = json.loads(row["preferred_categories"]) if row["preferred_categories"] else []
    except Exception:
        preferred_categories = []
    try:
        notification_preferences = json.loads(row["notification_preferences"]) if row["notification_preferences"] else {}
    except Exception:
        notification_preferences = {}
    return {
        "language": row["language"] or "English",
        "region": row["region"] or "Global",
        "theme": row["theme"] or "Dark",
        "preferred_categories": preferred_categories,
        "notification_preferences": notification_preferences,
        "updated_at": row["updated_at"] or _utc_now(),
    }


def upsert_user_preferences(user_id: str, prefs: Dict[str, Any]) -> None:
    conn = _get_conn()
    preferred_categories = json.dumps(prefs.get("preferred_categories") or [])
    notification_preferences = json.dumps(prefs.get("notification_preferences") or {})
    updated_at = _utc_now()
    conn.execute(
        """
        INSERT INTO user_preferences (user_id, language, region, theme, preferred_categories, notification_preferences, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            language=excluded.language,
            region=excluded.region,
            theme=excluded.theme,
            preferred_categories=excluded.preferred_categories,
            notification_preferences=excluded.notification_preferences,
            updated_at=excluded.updated_at
        """,
        (
            user_id,
            prefs.get("language"),
            prefs.get("region"),
            prefs.get("theme"),
            preferred_categories,
            notification_preferences,
            updated_at,
        ),
    )
    conn.commit()


def insert_user_feedback(event: Dict[str, Any]) -> None:
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO user_feedback (id, user_id, article_id, action, timestamp, context, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.get("feedback_id"),
            event.get("user_id"),
            event.get("article_id"),
            event.get("action"),
            event.get("timestamp"),
            json.dumps(event.get("context") or {}),
            _utc_now(),
        ),
    )
    conn.commit()


# --------------------
# Articles helpers (live ingestion)
# --------------------

def upsert_article(article: Dict[str, Any]) -> None:
    """
    Upsert an article row.

    Required keys in article dict:
    - id: stable unique id (e.g., telegram:@channel|123 or hash of URL)
    - title
    - source_name
    - source_url (canonical URL to content)
    - category (general|tech|finance|science or custom)
    - published_at (ISO8601)

    Optional keys:
    - thumbnail_url
    - relevance_score
    - processing_status
    - processing_progress
    """
    conn = _get_conn()
    conn.execute(
        """
        INSERT INTO articles (
            id, title, source_name, source_url, thumbnail_url, category,
            published_at, relevance_score, processing_status, processing_progress, group_key, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            title=excluded.title,
            source_name=excluded.source_name,
            source_url=excluded.source_url,
            thumbnail_url=excluded.thumbnail_url,
            category=excluded.category,
            published_at=excluded.published_at,
            relevance_score=COALESCE(excluded.relevance_score, articles.relevance_score),
            processing_status=COALESCE(excluded.processing_status, articles.processing_status),
            processing_progress=COALESCE(excluded.processing_progress, articles.processing_progress),
            group_key=COALESCE(excluded.group_key, articles.group_key)
        """,
        (
            article.get("id"),
            article.get("title"),
            article.get("source_name"),
            article.get("source_url"),
            article.get("thumbnail_url"),
            article.get("category"),
            article.get("published_at"),
            float(article.get("relevance_score")) if article.get("relevance_score") is not None else None,
            article.get("processing_status"),
            int(article.get("processing_progress")) if article.get("processing_progress") is not None else None,
            article.get("group_key"),
            _utc_now(),
        ),
    )
    conn.commit()


def get_articles(limit: int, offset: int = 0, category: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return articles ordered by published_at desc, optionally filtered by category."""
    conn = _get_conn()
    if category:
        cur = conn.execute(
            "SELECT * FROM articles WHERE LOWER(category)=LOWER(?) ORDER BY datetime(published_at) DESC LIMIT ? OFFSET ?",
            (category, limit, offset),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM articles ORDER BY datetime(published_at) DESC LIMIT ? OFFSET ?",
            (limit, offset),
        )
    rows = cur.fetchall()
    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append({k: r[k] for k in r.keys()})
    return result


def count_articles(category: Optional[str] = None) -> int:
    """Return the total count of articles, optionally filtered by category."""
    conn = _get_conn()
    if category:
        cur = conn.execute("SELECT COUNT(*) AS c FROM articles WHERE LOWER(category)=LOWER(?)", (category,))
    else:
        cur = conn.execute("SELECT COUNT(*) AS c FROM articles")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def get_article_by_id(article_id: str) -> Optional[Dict[str, Any]]:
    conn = _get_conn()
    cur = conn.execute("SELECT * FROM articles WHERE id = ?", (article_id,))
    r = cur.fetchone()
    if not r:
        return None
    return {k: r[k] for k in r.keys()}


def count_article_groups(category: Optional[str] = None) -> int:
    """Count distinct non-null group_key plus items without group_key (treated as separate)."""
    conn = _get_conn()
    if category:
        cur1 = conn.execute("SELECT COUNT(DISTINCT group_key) AS c FROM articles WHERE group_key IS NOT NULL AND LOWER(category)=LOWER(?)", (category,))
        cur2 = conn.execute("SELECT COUNT(*) AS c FROM articles WHERE group_key IS NULL AND LOWER(category)=LOWER(?)", (category,))
    else:
        cur1 = conn.execute("SELECT COUNT(DISTINCT group_key) AS c FROM articles WHERE group_key IS NOT NULL")
        cur2 = conn.execute("SELECT COUNT(*) AS c FROM articles WHERE group_key IS NULL")
    c1 = cur1.fetchone()
    c2 = cur2.fetchone()
    n1 = int(c1[0]) if c1 else 0
    n2 = int(c2[0]) if c2 else 0
    return n1 + n2


def get_articles_in_timeframe(secs: int, category: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return articles with published_at within last `secs`, optionally filtered by category."""
    conn = _get_conn()
    if category:
        cur = conn.execute(
            "SELECT * FROM articles WHERE LOWER(category)=LOWER(?) AND datetime(published_at) >= datetime('now', ? || ' seconds') ORDER BY datetime(published_at) DESC",
            (category, f'-{int(secs)}'),
        )
    else:
        cur = conn.execute(
            "SELECT * FROM articles WHERE datetime(published_at) >= datetime('now', ? || ' seconds') ORDER BY datetime(published_at) DESC",
            (f'-{int(secs)}',),
        )
    rows = cur.fetchall()
    return [{k: r[k] for k in r.keys()} for r in rows]


def get_group_representative(group_key: str, secs: Optional[int] = None, category: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Return the latest article in a group, optionally constrained by timeframe and category."""
    conn = _get_conn()
    if secs is not None and secs > 0:
        if category:
            cur = conn.execute(
                "SELECT * FROM articles WHERE group_key=? AND LOWER(category)=LOWER(?) AND datetime(published_at) >= datetime('now', ? || ' seconds') ORDER BY datetime(published_at) DESC LIMIT 1",
                (group_key, category, f'-{int(secs)}'),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM articles WHERE group_key=? AND datetime(published_at) >= datetime('now', ? || ' seconds') ORDER BY datetime(published_at) DESC LIMIT 1",
                (group_key, f'-{int(secs)}'),
            )
    else:
        if category:
            cur = conn.execute(
                "SELECT * FROM articles WHERE group_key=? AND LOWER(category)=LOWER(?) ORDER BY datetime(published_at) DESC LIMIT 1",
                (group_key, category),
            )
        else:
            cur = conn.execute(
                "SELECT * FROM articles WHERE group_key=? ORDER BY datetime(published_at) DESC LIMIT 1",
                (group_key,),
            )
    r = cur.fetchone()
    if not r:
        return None
    return {k: r[k] for k in r.keys()}


def get_grouped_articles(limit: int, offset: int = 0, category: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return one representative per dedup group, ordered by latest published time.

    Groups are defined by COALESCE(group_key, id) to treat items without group_key
    as separate singleton groups.
    """
    conn = _get_conn()
    params: List[Any] = []
    where = ""
    if category:
        where = "WHERE LOWER(category)=LOWER(?)"
        params.append(category)
    # Use a CTE to get latest published time per group id
    sql = f"""
        WITH grouped AS (
            SELECT COALESCE(group_key, id) AS gid, MAX(datetime(published_at)) AS latest
            FROM articles
            {where}
            GROUP BY gid
        )
        SELECT a.*
        FROM articles a
        JOIN grouped g ON g.gid = COALESCE(a.group_key, a.id) AND datetime(a.published_at) = g.latest
        {where}
        ORDER BY datetime(a.published_at) DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])
    cur = conn.execute(sql, tuple(params))
    rows = cur.fetchall()
    return [{k: r[k] for k in r.keys()} for r in rows]