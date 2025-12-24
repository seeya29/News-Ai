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


# Use async httpx client per test


def make_token(user_id: str, role: str = "user", ttl: int = 3600) -> str:
    header = {"alg": "none", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": int(time.time()) + ttl}
    def b64url(data: bytes) -> str:
        import base64 as b64
        return b64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")
    h = b64url(json.dumps(header).encode("utf-8"))
    p = b64url(json.dumps(payload).encode("utf-8"))
    return f"{h}.{p}.signature"


def auth_headers(role: str):
    return {"Authorization": f"Bearer {make_token('validator', role)}"}


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