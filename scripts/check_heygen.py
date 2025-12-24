import os
import sys
import json
from typing import Optional

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import httpx  # type: ignore


def pick_latest_wav(tts_dir: str) -> Optional[str]:
    try:
        files = [f for f in os.listdir(tts_dir) if f.lower().endswith('.wav')]
        if not files:
            return None
        files.sort(key=lambda f: os.path.getmtime(os.path.join(tts_dir, f)), reverse=True)
        return files[0]
    except Exception:
        return None


def main() -> None:
    base_url = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    api_root = (os.getenv("HEYGEN_API_BASE_URL") or os.getenv("HEYGEN_API_BASE") or "https://api.heygen.com").rstrip("/")
    api_key = os.getenv("HEYGEN_API_KEY")
    avatar_id = os.getenv("HEYGEN_AVATAR_ID")

    tts_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "single_pipeline", "data", "tts"))
    fname = pick_latest_wav(tts_dir)
    if not fname:
        print(json.dumps({"ok": False, "error": "no_wav_found", "tts_dir": tts_dir}))
        return
    audio_route = f"/data/tts/{fname}"
    audio_url = base_url + audio_route

    if not api_key or not avatar_id:
        print(json.dumps({"ok": False, "error": "missing_env", "missing": {"HEYGEN_API_KEY": not bool(api_key), "HEYGEN_AVATAR_ID": not bool(avatar_id)}, "audio_url": audio_url}))
        return

    headers = {"X-Api-Key": api_key, "Accept": "application/json", "Content-Type": "application/json"}
    payload = {
        "test": False,
        "character": {"type": "avatar", "avatar_id": avatar_id},
        "voice": {"type": "audio", "audio_url": audio_url},
        "video_settings": {"aspect_ratio": "16:9"},
    }

    gen_url = api_root + "/v2/video/generate"
    try:
        r = httpx.post(gen_url, headers=headers, json=payload, timeout=30)
        data = {}
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text[:200]}
        out = {
            "ok": r.status_code < 300,
            "status_code": r.status_code,
            "endpoint": gen_url,
            "avatar_id": avatar_id,
            "audio_url": audio_url,
            "response": {k: data.get(k) for k in ("message", "error", "code", "data", "video_id") if k in data},
        }
        print(json.dumps(out, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": "request_failed", "message": str(e)}))

    # Also check if the avatar_id exists in /v2/avatars
    try:
        headers2 = {"X-Api-Key": api_key, "Accept": "application/json"}
        r2 = httpx.get(api_root + "/v2/avatars", headers=headers2, timeout=30)
        ok2 = r2.status_code < 300
        found = False
        names = []
        ids = []
        try:
            j = r2.json()
            items = j.get("data") if isinstance(j.get("data"), (list, dict)) else []
            if isinstance(items, dict):
                items = items.get("avatars") or items.get("items") or []
            if not items:
                items = j.get("avatars") or []
            for it in items:
                aid = str(it.get("avatar_id") or it.get("id") or "")
                if aid == avatar_id:
                    found = True
                if aid:
                    ids.append(aid)
                n = str(it.get("name") or it.get("pose_name") or "")
                if n:
                    names.append(n)
        except Exception:
            pass
        print(json.dumps({
            "avatars_list_ok": ok2,
            "status_code": r2.status_code,
            "avatar_found": found,
            "sample_ids": ids[:5],
            "sample_names": names[:5],
        }, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"ok": False, "error": "avatars_list_failed", "message": str(e)}))


if __name__ == "__main__":
    main()
