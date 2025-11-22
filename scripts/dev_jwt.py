import os
import json
import time
import argparse
import base64


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def make_token(user_id: str, role: str = "admin", ttl_seconds: int = 12 * 3600) -> str:
    now = int(time.time())
    exp = now + int(ttl_seconds)
    header = {"alg": "none", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": exp}
    h = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{h}.{p}."


def main():
    parser = argparse.ArgumentParser(description="Generate a dev JWT compatible with the News-Ai server")
    parser.add_argument("--user-id", default=os.environ.get("NEWSAI_USER_ID", "demo"), help="User ID to embed in the token")
    parser.add_argument("--role", default=os.environ.get("NEWSAI_ROLE", "admin"), choices=["user", "admin"], help="Role to embed in the token")
    parser.add_argument("--ttl", type=int, default=int(os.environ.get("NEWSAI_TTL", 12 * 3600)), help="Token TTL in seconds (default 12h)")
    parser.add_argument("--print-curl", action="store_true", help="Print a curl example with the Authorization header")
    args = parser.parse_args()

    token = make_token(args.user_id, role=args.role, ttl_seconds=args.ttl)
    print(token)
    if args.print_curl:
        print("\nExample: curl -H \"Authorization: Bearer %s\" http://localhost:8000/api/health" % token)


if __name__ == "__main__":
    main()

import time
import json
import hmac
import base64
import argparse
from hashlib import sha256


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def sign_hs256(secret: str, header: dict, payload: dict) -> str:
    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), signing_input, sha256).digest()
    return f"{header_b64}.{payload_b64}.{b64url(sig)}"


def main():
    parser = argparse.ArgumentParser(description="Generate dev JWT token (HS256)")
    parser.add_argument("user_id", help="User ID claim")
    parser.add_argument("role", choices=["admin", "editor", "user"], help="Role claim")
    parser.add_argument("--ttl", type=int, default=3600, help="Token TTL seconds (default 3600)")
    args = parser.parse_args()

    secret = os.getenv("JWT_SECRET")
    if not secret:
        raise SystemExit("JWT_SECRET env var not set")

    header = {"alg": "HS256", "typ": "JWT"}
    payload = {
        "user_id": args.user_id,
        "role": args.role,
        "exp": int(time.time()) + int(args.ttl),
    }
    token = sign_hs256(secret, header, payload)
    print(token)


if __name__ == "__main__":
    main()