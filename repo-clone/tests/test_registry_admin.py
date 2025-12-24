import base64
import json
import time
import os
from typing import Dict, Any

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


# Use async client per test to avoid transport state issues


def make_token(user_id: str, role: str = "user", ttl: int = 3600) -> str:
    header = {"alg": "none", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": int(time.time()) + ttl}
    def b64url(data: bytes) -> str:
        return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")
    h = b64url(json.dumps(header).encode("utf-8"))
    p = b64url(json.dumps(payload).encode("utf-8"))
    return f"{h}.{p}.signature"


def auth_headers(role: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {make_token('test', role)}"}


def test_get_registry_requires_admin():
    async def _run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=APP), base_url="http://test") as client:
            r = await client.get("/api/admin/feeds/registry", headers=auth_headers("user"))
            assert r.status_code == 403
    asyncio.run(_run())


def test_post_and_reload_registry_admin_flow(tmp_path):
    # Upload feeds using JSON body
    feeds = {
        "feeds": [
            {"id": "telegram_tech", "type": "telegram", "channel": "@technews", "cadence_seconds": 60},
            {"id": "x_markets", "type": "x", "handle": "@EconomicTimes", "cadence_seconds": 30},
            {"id": "yt_bbc", "type": "youtube_rss", "channel_id": "UC16niRr50-MSBwiO3YDb3RA", "cadence_seconds": 900},
        ]
    }
    async def _run():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=APP), base_url="http://test") as client:
            r = await client.post("/api/admin/feeds/registry", headers=auth_headers("admin"), json=feeds)
            assert r.status_code == 200, r.text
            data = r.json()

            # GET registry
            r2 = await client.get("/api/admin/feeds/registry", headers=auth_headers("admin"))
            assert r2.status_code == 200
            reg = r2.json()["registry"]

            # Reload
            r3 = await client.post("/api/admin/feeds/reload", headers=auth_headers("admin"))
            assert r3.status_code == 200
            summary = r3.json()
        return data, reg, summary
    data, reg, summary = asyncio.run(_run())
    assert data["result"] == "ok"
    assert data["feeds"] == 3
    assert isinstance(data.get("warnings"), list)
    assert isinstance(reg.get("feeds"), list)
    assert len(reg["feeds"]) == 3
    assert summary["result"] == "ok"
    assert summary["feeds"] == 3
    # sources.json should exist
    sources_file = summary["sources_file"]
    assert os.path.exists(sources_file)