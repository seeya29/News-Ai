import asyncio
import datetime
from typing import List, Optional

from server.db import upsert_article
from single_pipeline.rag_client import RAGClient


def _utc_iso(dt: Optional[datetime.datetime]) -> str:
    d = dt or datetime.datetime.now(datetime.timezone.utc)
    if d.tzinfo is None:
        d = d.replace(tzinfo=datetime.timezone.utc)
    return d.isoformat()


async def fetch_telegram_channels(channels: List[str], api_id: Optional[int], api_hash: Optional[str], limit_per_channel: int = 20) -> int:
    """Fetch recent messages from Telegram channels using Telethon and upsert into DB.

    Returns the number of articles ingested.
    """
    try:
        from telethon import TelegramClient
        from telethon.errors import FloodWaitError
    except Exception:
        print("[ingest:telegram] telethon not installed; skipping.")
        return 0

    if not api_id or not api_hash:
        print("[ingest:telegram] Missing TELEGRAM_API_ID/TELEGRAM_API_HASH; skipping.")
        return 0

    ingested = 0
    rag = RAGClient()
    # Use a local session file so we don't re-auth every run
    client = TelegramClient(".telegram_session", api_id, api_hash)
    await client.start()
    for chan in channels:
        try:
            async for msg in client.iter_messages(chan, limit=limit_per_channel):
                if not (getattr(msg, "message", None) or getattr(msg, "text", None)) and not getattr(msg, "media", None):
                    continue
                text = (getattr(msg, "message", None) or getattr(msg, "text", None) or "").strip()
                # Telegram messages may not have canonical URLs; prefer deep links when available
                url = None
                try:
                    if hasattr(msg, "link") and msg.link:
                        url = msg.link
                except Exception:
                    pass
                # Build a stable id from channel and message id
                art_id = f"telegram:{chan}|{msg.id}"
                # Compute group key using full text and published time
                gk = None
                try:
                    gk = rag.assign_group_key(title=text, body="", published_at_iso=_utc_iso(getattr(msg, "date", None)), category="tech")
                except Exception:
                    gk = None

                article = {
                    "id": art_id,
                    "title": text[:140] or "Telegram Update",
                    "source_name": f"{chan}",
                    "source_url": url or f"https://t.me/{chan.lstrip('@')}/{msg.id}",
                    "thumbnail_url": None,
                    "category": "tech",
                    "published_at": _utc_iso(getattr(msg, "date", None)),
                    "relevance_score": 0.75,
                    "processing_status": "ingested",
                    "processing_progress": 10,
                    "group_key": gk,
                }
                upsert_article(article)
                ingested += 1
        except FloodWaitError as e:
            print(f"[ingest:telegram] Flood wait for {chan}: {e}")
            await asyncio.sleep(int(getattr(e, "seconds", 30)))
        except Exception as e:
            print(f"[ingest:telegram] Error fetching {chan}: {e}")
            continue
    await client.disconnect()
    return ingested


def fetch_x_handles(handles: List[str], bearer_token: Optional[str], limit_per_handle: int = 20) -> int:
    """Fetch recent tweets for provided handles using Tweepy (API v2) and upsert into DB.
    Falls back to snscrape if tweepy or credentials are unavailable.
    Returns count ingested.
    """
    try:
        import tweepy
    except Exception:
        tweepy = None

    if tweepy and bearer_token:
        try:
            client = tweepy.Client(bearer_token=bearer_token, wait_on_rate_limit=True)
        except Exception as e:
            print(f"[ingest:x] Failed to init tweepy client: {e}; will try snscrape.")
            client = None
    else:
        client = None

    total = 0
    rag = RAGClient()
    if client:
        for handle in handles:
            try:
                # Get user by username
                user = client.get_user(username=handle)
                if not user or not getattr(user, "data", None):
                    continue
                uid = user.data.id
                tweets = client.get_users_tweets(id=uid, max_results=min(limit_per_handle, 100), tweet_fields=["created_at"])
                for tw in getattr(tweets, "data", []) or []:
                    art_id = f"x:{handle}|{tw.id}"
                    url = f"https://twitter.com/{handle}/status/{tw.id}"
                    # Compute group key on tweet text
                    full_text = (tw.text or "").strip()
                    gk = None
                    try:
                        gk = rag.assign_group_key(title=full_text, body="", published_at_iso=_utc_iso(getattr(tw, "created_at", None)), category="tech")
                    except Exception:
                        gk = None

                    article = {
                        "id": art_id,
                        "title": full_text[:140] or "Tweet",
                        "source_name": f"@{handle}",
                        "source_url": url,
                        "thumbnail_url": None,
                        "category": "tech",
                        "published_at": _utc_iso(getattr(tw, "created_at", None)),
                        "relevance_score": 0.75,
                        "processing_status": "ingested",
                        "processing_progress": 10,
                        "group_key": gk,
                    }
                    upsert_article(article)
                    total += 1
            except Exception as e:
                print(f"[ingest:x] Error for @{handle}: {e}")
    else:
        # Fallback to snscrape if available
        try:
            import subprocess, json
            for handle in handles:
                cmd = [
                    "snscrape",
                    "--max-results", str(limit_per_handle),
                    "--jsonl",
                    f"twitter-user", handle,
                ]
                try:
                    out = subprocess.check_output(cmd, text=True)
                except Exception as e:
                    print(f"[ingest:x] snscrape failed for {handle}: {e}")
                    continue
                for line in out.splitlines():
                    try:
                        tw = json.loads(line)
                    except Exception:
                        continue
                    art_id = f"x:{handle}|{tw.get('id')}"
                    url = tw.get("url")
                    full_text = (tw.get("content") or "").strip()
                    gk = None
                    try:
                        published = tw.get("date")
                        dt = datetime.datetime.fromisoformat(published) if published else None
                        gk = rag.assign_group_key(title=full_text, body="", published_at_iso=_utc_iso(dt), category="tech")
                    except Exception:
                        gk = None
                    article = {
                        "id": art_id,
                        "title": full_text[:140] or "Tweet",
                        "source_name": f"@{handle}",
                        "source_url": url,
                        "thumbnail_url": None,
                        "category": "tech",
                        "published_at": _utc_iso(datetime.datetime.fromisoformat(tw.get("date"))) if tw.get("date") else _utc_iso(None),
                        "relevance_score": 0.75,
                        "processing_status": "ingested",
                        "processing_progress": 10,
                        "group_key": gk,
                    }
                    upsert_article(article)
                    total += 1
        except Exception:
            print("[ingest:x] Neither tweepy nor snscrape available; skipping.")
    return total


def fetch_youtube_channels(channel_ids: List[str], api_key: Optional[str], limit_per_channel: int = 20) -> int:
    """Fetch recent videos from YouTube. Uses Data API if available, else RSS.
    Returns count ingested.
    """
    total = 0
    rag = RAGClient()
    # Try Data API first
    if api_key:
        try:
            from googleapiclient.discovery import build
            yt = build("youtube", "v3", developerKey=api_key)
            for cid in channel_ids:
                req = yt.search().list(part="snippet", channelId=cid, order="date", maxResults=min(limit_per_channel, 50))
                resp = req.execute()
                for item in resp.get("items", []) or []:
                    if item.get("id", {}).get("kind") != "youtube#video":
                        continue
                    vid = item.get("id", {}).get("videoId")
                    sn = item.get("snippet", {})
                    art_id = f"yt:{cid}|{vid}"
                    url = f"https://www.youtube.com/watch?v={vid}"
                    # Compute group key using title only for video
                    title = sn.get("title") or "YouTube Video"
                    gk = None
                    try:
                        gk = rag.assign_group_key(title=title, body="", published_at_iso=sn.get("publishedAt") or _utc_iso(None), category="tech")
                    except Exception:
                        gk = None

                    article = {
                        "id": art_id,
                        "title": title,
                        "source_name": sn.get("channelTitle") or cid,
                        "source_url": url,
                        "thumbnail_url": (sn.get("thumbnails", {}).get("medium", {}) or {}).get("url"),
                        "category": "tech",
                        "published_at": sn.get("publishedAt") or _utc_iso(None),
                        "relevance_score": 0.75,
                        "processing_status": "ingested",
                        "processing_progress": 10,
                        "group_key": gk,
                    }
                    upsert_article(article)
                    total += 1
            return total
        except Exception as e:
            print(f"[ingest:youtube] Data API not available: {e}; falling back to RSS.")

    # RSS fallback
    try:
        import feedparser
    except Exception:
        print("[ingest:youtube] feedparser not installed; skipping.")
        return total

    for cid in channel_ids:
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={cid}"
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.get("entries", [])[:limit_per_channel]:
                vid = entry.get("yt_videoid") or entry.get("id")
                url = entry.get("link")
                published = entry.get("published")
                try:
                    dt = datetime.datetime.fromisoformat(published.replace("Z", "+00:00")) if published else None
                except Exception:
                    dt = None
                art_id = f"yt:{cid}|{vid}"
                # Compute group key using title only for RSS item
                title = entry.get("title") or "YouTube Video"
                gk = None
                try:
                    gk = rag.assign_group_key(title=title, body="", published_at_iso=_utc_iso(dt), category="tech")
                except Exception:
                    gk = None

                article = {
                    "id": art_id,
                    "title": title,
                    "source_name": entry.get("author") or cid,
                    "source_url": url,
                    "thumbnail_url": None,
                    "category": "tech",
                    "published_at": _utc_iso(dt),
                    "relevance_score": 0.75,
                    "processing_status": "ingested",
                    "processing_progress": 10,
                    "group_key": gk,
                }
                upsert_article(article)
                total += 1
        except Exception as e:
            print(f"[ingest:youtube] RSS error for {cid}: {e}")
            continue
    return total