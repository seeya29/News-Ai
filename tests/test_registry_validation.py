import base64
import json
import time
import httpx
import asyncio
import importlib.util
import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
APP_PATH = os.path.join(ROOT, "server", "app.py")
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
spec = importlib.util.spec_from_file_location("app_module", APP_PATH)
app_module = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(app_module)  # type: ignore
APP = app_module.APP
AuthContext = app_module.AuthContext
require_auth = app_module.require_auth

# Mock authentication
def mock_auth():
    return AuthContext(user_id="validator", role="admin", exp=9999999999)

APP.dependency_overrides[require_auth] = mock_auth


def auth_headers(role: str):
    return {}


def test_unknown_field_is_ignored_with_warning():
    feeds = {
        "feeds": [
            {
                "id": "telegram_tech",
                "type": "telegram",
                "channel": "@technews",
                "cadence_seconds": 60,
                "foo": "bar",  # unknown
            }
        ]
    }
    async def _run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=APP), base_url="http://test") as client:
            r = await client.post("/api/admin/feeds/registry", headers=auth_headers("admin"), json=feeds)
            assert r.status_code == 200, r.text
            return r.json()
    data = asyncio.run(_run())
    assert data["feeds"] == 1
    assert isinstance(data.get("warnings"), list)
    assert any("Unknown field" in (w.get("warning") or "") for w in data["warnings"])


def test_rss_and_api_feeds_supported():
    feeds = {
        "feeds": [
            {
                "id": "bbc_tech_rss",
                "type": "rss",
                "name": "BBC Technology",
                "feed_url": "http://feeds.bbci.co.uk/news/technology/rss.xml",
                "cadence_seconds": 3600
            },
            {
                "id": "hn_top_api",
                "type": "api",
                "name": "HackerNews Top",
                "url": "https://hacker-news.firebaseio.com/v0/topstories.json",
                "params": {"print": "pretty"},
                "cadence_seconds": 300
            }
        ]
    }
    async def _run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=APP), base_url="http://test") as client:
            r = await client.post("/api/admin/feeds/registry", headers=auth_headers("admin"), json=feeds)
            assert r.status_code == 200, r.text
            return r.json()
    data = asyncio.run(_run())
    assert data["feeds"] == 2
    # Expect no warnings for valid fields
    if "warnings" in data:
        # Filter out warnings related to 'params' if my implementation considered it unknown, 
        # but I explicitly added support for 'params' in registry.py so it should be fine.
        # Wait, did I add 'params' to the allowed fields in registry.py?
        # Let's check registry.py before asserting zero warnings.
        pass
    
    # Actually, I should check if 'params' is passed through.
    # But this test only checks the API response which returns count and warnings.
    # To verifying the sources.json is updated, I would need to mock save_registry or check the file.
    # But the API call calls 'validate_feeds' and 'save_registry_yaml' and 'hot_reload'.
    # 'hot_reload' writes to sources.json.
    
    if "warnings" in data and len(data["warnings"]) > 0:
        print(f"Warnings: {data['warnings']}")
        # Fail if warnings are unexpected
        assert len(data["warnings"]) == 0, f"Unexpected warnings: {data['warnings']}"
