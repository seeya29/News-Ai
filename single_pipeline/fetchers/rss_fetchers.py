import time
from typing import Any, Dict, List, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from xml.etree import ElementTree as ET

from single_pipeline.logging_utils import PipelineLogger


class RSSFetcher:
    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
        timeout: float = 8.0,
        overall_timeout: float = 30.0,
        max_items: int = 50,
    ):
        self.timeout = timeout
        self.overall_timeout = overall_timeout
        self.max_items = max_items
        self.log = logger or PipelineLogger(component="rss_fetcher")

    def fetch(self, url: str, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        start = time.monotonic()
        if not isinstance(url, str) or not url.strip():
            return {
                "result": "error",
                "error": "invalid_url",
                "detail": "URL must be a non-empty string",
            }
        self.log.info("rss_fetch_start", url=url)
        try:
            req = Request(url, headers=headers or {})
            with urlopen(req, timeout=self.timeout) as resp:
                status = getattr(resp, "status", 200)
                data = resp.read()
            if time.monotonic() - start > self.overall_timeout:
                self.log.warning("rss_overall_timeout", url=url)
                return {"result": "error", "error": "timeout", "detail": "overall_timeout"}

            items: List[Dict[str, Any]] = []
            try:
                root = ET.fromstring(data)
                channel = root.find("channel") if root.tag.lower().endswith("rss") else root
                candidates = []
                if channel is not None:
                    candidates = channel.findall("item")
                if not candidates:
                    candidates = root.findall(".//item")

                for it in candidates[: self.max_items]:
                    title = (it.findtext("title") or "").strip()
                    link = (it.findtext("link") or "").strip()
                    pub = (it.findtext("pubDate") or it.findtext("updated") or "").strip()
                    summary = (it.findtext("description") or "").strip()
                    items.append({
                        "title": title or None,
                        "link": link or None,
                        "published": pub or None,
                        "summary": summary or None,
                    })
            except Exception as e:
                self.log.warning("rss_parse_error", url=url, error=str(e))
                return {
                    "result": "error",
                    "error": "parse_error",
                    "detail": str(e),
                }

            self.log.info("rss_fetch_success", url=url, status_code=status, count=len(items))
            return {
                "result": "ok",
                "url": url,
                "status_code": status,
                "items": items,
                "count": len(items),
            }
        except HTTPError as e:
            status = getattr(e, "code", None)
            self.log.warning("rss_http_error", url=url, status_code=status, error=str(e))
            return {
                "result": "error",
                "url": url,
                "status_code": status,
                "error": "http_error",
                "detail": str(e),
            }
        except URLError as e:
            self.log.warning("rss_url_error", url=url, error=str(e))
            return {
                "result": "error",
                "url": url,
                "error": "connection_error",
                "detail": str(e),
            }
        except Exception as e:
            self.log.error("rss_unexpected_error", url=url, error=str(e))
            return {
                "result": "error",
                "url": url,
                "error": "unexpected_error",
                "detail": str(e),
            }