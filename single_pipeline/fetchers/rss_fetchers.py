import time
from typing import Any, Dict, List, Optional

import feedparser
from base_fetcher import BaseFetcher
from logging_utils import PipelineLogger


class RSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        url = params.get("feed_url") or params.get("url")
        if not url:
            return [{"title": "RSSFetcher misconfigured", "body": "Missing feed_url/url", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "rss", "count": len(items), "url": url})
            return items
        except Exception as e:
            return [{"title": "RSSFetcher error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class YouTubeRSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        channel_id = params.get("channel_id")
        feed_url = params.get("feed_url") or (f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}" if channel_id else None)
        if not feed_url:
            return [{"title": "YouTubeRSS misconfigured", "body": "Missing feed_url or channel_id", "timestamp": None, "raw": {"error": "missing_params"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "youtube_rss", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "YouTubeRSS error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class NitterRSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        feed_url = self.cfg.get("params", {}).get("feed_url")
        if not feed_url:
            return [{"title": "NitterRSS misconfigured", "body": "Missing feed_url", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "x_nitter", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "NitterRSS error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class RedditRSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        subreddit = params.get("subreddit")
        feed_url = params.get("feed_url") or (f"https://www.reddit.com/r/{subreddit}/.rss" if subreddit else None)
        if not feed_url:
            return [{"title": "RedditRSS misconfigured", "body": "Missing feed_url or subreddit", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "reddit_rss", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "RedditRSS error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class MediumRSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        tag = params.get("tag")
        feed_url = params.get("feed_url") or (f"https://medium.com/feed/tag/{tag}" if tag else None)
        if not feed_url:
            return [{"title": "MediumRSS misconfigured", "body": "Missing feed_url or tag", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "medium_rss", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "MediumRSS error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class GitHubAtomFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        repo = params.get("repo")
        feed_url = params.get("feed_url") or (f"https://github.com/{repo}/commits.atom" if repo else None)
        if not feed_url:
            return [{"title": "GitHubAtom misconfigured", "body": "Missing feed_url or repo", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "github_atom", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "GitHubAtom error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class StackOverflowRSSFetcher(BaseFetcher):
    def fetch(self, limit: int = 10, logger: Optional[PipelineLogger] = None) -> List[Dict[str, Any]]:
        params = self.cfg.get("params", {})
        tag = params.get("tag")
        feed_url = params.get("feed_url") or (f"https://stackoverflow.com/feeds/tag?tagnames={tag}&sort=newest" if tag else None)
        if not feed_url:
            return [{"title": "StackOverflowRSS misconfigured", "body": "Missing feed_url or tag", "timestamp": None, "raw": {"error": "missing_feed_url"}}]
        try:
            d = feedparser.parse(feed_url)
            items: List[Dict[str, Any]] = []
            for entry in d.entries[:limit]:
                items.append({
                    "title": entry.get("title", "Untitled"),
                    "body": entry.get("summary", ""),
                    "timestamp": entry.get("published", None),
                    "raw": {"entry": dict(entry)}
                })
            if logger:
                logger.log_event("fetch", {"connector": "stackoverflow_rss", "count": len(items), "url": feed_url})
            return items
        except Exception as e:
            return [{"title": "StackOverflowRSS error", "body": str(e), "timestamp": None, "raw": {"error": str(e)}}]


class GenericRSSFetcher(RSSFetcher):
    pass