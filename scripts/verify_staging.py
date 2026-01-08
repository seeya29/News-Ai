import httpx
import json
import sys

BASE_URL = "http://localhost:8000"
TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VyX2lkIjoic3RhZ2luZ191c2VyIiwicm9sZSI6ImFkbWluIiwiZXhwIjoxNzY3NTA0NjMyfQ.E-ZVvTPWyGkyQwsco013VHYx2uD-oLSC64jkMX710u0"

def test_process():
    print("Testing /process endpoint...")
    headers = {"Authorization": f"Bearer {TOKEN}"}
    payload = {
        "registry": "single",
        "category": "general",
        "limit_preview": 5
    }
    
    try:
        resp = httpx.post(f"{BASE_URL}/process", json=payload, headers=headers, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            print("SUCCESS: /process returned 200 OK")
            print(f"Counts: {data.get('counts')}")
            preview = data.get("preview", [])
            if preview:
                print(f"First item preview: {json.dumps(preview[0], indent=2)}")
                # Verify schema keys
                keys = preview[0].keys()
                required = ["lang", "tone", "variants"] # keys used in server/app.py
                missing = [k for k in required if k not in keys]
                if missing:
                    print(f"WARNING: Missing keys in preview: {missing}")
                else:
                    print("Schema keys verified in preview.")
            else:
                print("WARNING: No items in preview (maybe no input data?)")
        else:
            print(f"FAILED: Status {resp.status_code}")
            print(resp.text)
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_process()
