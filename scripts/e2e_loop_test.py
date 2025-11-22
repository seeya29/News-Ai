import os
import json
import time
import hmac
import base64
from hashlib import sha256

BASE_URL = os.getenv("NEWSAI_BASE_URL", "http://127.0.0.1:8000")


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def sign_hs256(secret: str, header: dict, payload: dict) -> str:
    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), signing_input, sha256).digest()
    return f"{header_b64}.{payload_b64}.{b64url(sig)}"


def make_dev_token(user_id: str = "e2e_user", role: str = "user", ttl: int = 900) -> str:
    secret = os.getenv("JWT_SECRET")
    if not secret:
        raise SystemExit("Set JWT_SECRET env var to generate dev token")
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": int(time.time()) + int(ttl)}
    return sign_hs256(secret, header, payload)


def http_post(path: str, token: str, body: dict):
    import urllib.request
    import urllib.error
    req = urllib.request.Request(
        url=f"{BASE_URL}{path}",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
            return resp.status, json.loads(data.decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, {"error": "http_error", "detail": e.reason}
    except Exception as e:
        return 0, {"error": "network_error", "detail": str(e)}


def main():
    token = make_dev_token()
    print("Base:", BASE_URL)

    # 1) Fetch
    code, fetch = http_post("/fetch", token, {"registry": "single", "category": "general", "limit_preview": 5})
    print("Fetch:", code, f"count={fetch.get('count')} err={fetch.get('error')}")
    if fetch.get("files", {}).get("items"):
        try:
            mt = os.path.getmtime(fetch["files"]["items"])
            print("  items file age (s):", round(time.time() - mt, 2))
        except Exception:
            pass
    preview = fetch.get("preview") or []
    if code != 200:
        print("Fetch failed:", fetch)
        return 1

    # 2) Process
    code, proc = http_post("/process", token, {"registry": "single", "category": "general", "limit_preview": 5})
    print("Process:", code, f"counts={proc.get('counts')} err={proc.get('error')}")
    if proc.get("files", {}).get("filtered"):
        try:
            mt = os.path.getmtime(proc["files"]["filtered"])
            print("  filtered file age (s):", round(time.time() - mt, 2))
        except Exception:
            pass
    proc_preview = proc.get("preview") or []
    if code != 200:
        print("Process failed:", proc)
        return 1

    # 3) Voice
    code, voice = http_post("/voice", token, {"registry": "single", "category": "general", "voice": "en-US-Neural-1"})
    print("Voice:", code, f"count={voice.get('count')} err={voice.get('error')}")
    if code != 200:
        print("Voice failed:", voice)
        return 1

    # 4) Feedback
    if proc_preview or preview:
        item = (proc_preview or preview)[0]
        article_id = item.get("id") or (item.get("title") or "")[:64]
        if article_id:
            fb_body = {
                "user_id": "e2e_user",
                "article_id": article_id,
                "action": "like",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "context": {
                    "lang": item.get("lang"),
                    "audience": item.get("audience"),
                    "tone": item.get("tone"),
                    "device": "desktop",
                    "session_id": "e2e",
                },
            }
            code, fb = http_post("/feedback", token, fb_body)
            print("Feedback:", code, f"id={fb.get('feedback_id')} err={fb.get('error')}")
        else:
            print("Feedback: skipped (missing article_id)")
    else:
        print("Feedback: skipped (no preview items)")

    # Summary
    print("\nE2E Summary:\n- Fetched:", fetch.get("count"), "items\n- Scripts:", proc.get("counts", {}).get("scripts"), "\n- Voice:", voice.get("count"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())