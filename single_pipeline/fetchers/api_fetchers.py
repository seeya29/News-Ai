import json
import time
from typing import Any, Dict, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from single_pipeline.logging_utils import PipelineLogger, StageLogger


class DomainAPIFetcher:
    def __init__(
        self,
        logger: Optional[PipelineLogger] = None,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.log = logger or PipelineLogger(component="domain_api_fetcher")

    def fetch(self, url: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        if not isinstance(url, str) or not url.strip():
            return {
                "result": "error",
                "error": "invalid_url",
                "detail": "URL must be a non-empty string",
            }

        final_url = url
        if params:
            try:
                from urllib.parse import urlencode
                query = urlencode(params, doseq=True)
                sep = "&" if "?" in url else "?"
                final_url = f"{url}{sep}{query}"
            except Exception as e:
                self.log.warning("api_params_encode_failed", url=url, error=str(e))

        self.log.info("api_fetch_start", url=final_url)
        run = StageLogger(source="domain_api", category="tech", meta={"url": final_url})
        run.start("fetch_api", meta={"timeout": self.timeout, "max_retries": self.max_retries})
        attempt = 0
        while attempt <= self.max_retries:
            try:
                req = Request(final_url, headers=headers or {})
                with urlopen(req, timeout=self.timeout) as resp:
                    ctype = resp.info().get_content_type()
                    status = getattr(resp, "status", 200)
                    data = resp.read()
                parsed: Any
                if ctype == "application/json":
                    parsed = json.loads(data.decode("utf-8", errors="replace"))
                else:
                    try:
                        parsed = json.loads(data.decode("utf-8", errors="replace"))
                        ctype = "application/json"
                    except Exception:
                        parsed = data.decode("utf-8", errors="replace")

                self.log.info("api_fetch_success", url=final_url, status_code=status, content_type=ctype)
                run.update("fetch_api", progress=100, meta={"status_code": status, "content_type": ctype})
                run.complete("fetch_api", meta={"status_code": status, "content_type": ctype})
                run.end_run("completed")
                return {
                    "result": "ok",
                    "url": final_url,
                    "status_code": status,
                    "data": parsed,
                    "meta": {"content_type": ctype, "attempts": attempt},
                }
            except HTTPError as e:
                status = getattr(e, "code", None)
                detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
                self.log.warning("api_fetch_http_error", url=final_url, status_code=status, attempt=attempt, error=str(e))
                try:
                    run.update("fetch_api", progress=min(99, int(100 * (attempt + 1) / (self.max_retries + 1))), meta={"attempt": attempt})
                except Exception:
                    pass
                if status and status >= 500 and attempt < self.max_retries:
                    time.sleep(self.backoff_factor * (2 ** attempt))
                    attempt += 1
                    continue
                try:
                    run.error("fetch_api", error_code="http_error", error_message=str(e), meta={"status_code": status, "attempt": attempt})
                    run.end_run("failed")
                except Exception:
                    pass
                return {
                    "result": "error",
                    "url": final_url,
                    "status_code": status,
                    "error": "http_error",
                    "detail": detail,
                    "attempts": attempt,
                }
            except URLError as e:
                self.log.warning("api_fetch_url_error", url=final_url, attempt=attempt, error=str(e))
                try:
                    run.update("fetch_api", progress=min(99, int(100 * (attempt + 1) / (self.max_retries + 1))), meta={"attempt": attempt})
                except Exception:
                    pass
                if attempt < self.max_retries:
                    time.sleep(self.backoff_factor * (2 ** attempt))
                    attempt += 1
                    continue
                try:
                    run.error("fetch_api", error_code="connection_error", error_message=str(e), meta={"attempt": attempt})
                    run.end_run("failed")
                except Exception:
                    pass
                return {
                    "result": "error",
                    "url": final_url,
                    "error": "connection_error",
                    "detail": str(e),
                    "attempts": attempt,
                }
            except Exception as e:
                self.log.error("api_fetch_unexpected_error", url=final_url, attempt=attempt, error=str(e))
                try:
                    run.error("fetch_api", error_code="unexpected_error", error_message=str(e), meta={"attempt": attempt})
                    run.end_run("failed")
                except Exception:
                    pass
                return {
                    "result": "error",
                    "url": final_url,
                    "error": "unexpected_error",
                    "detail": str(e),
                    "attempts": attempt,
                }