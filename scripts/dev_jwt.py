import os
import json
import time
import argparse
import base64
import hmac
from hashlib import sha256


def b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def make_unsigned_token(user_id: str, role: str = "admin", ttl_seconds: int = 12 * 3600) -> str:
    """Generate an unsigned JWT (alg=none). For local debugging only."""
    now = int(time.time())
    exp = now + int(ttl_seconds)
    header = {"alg": "none", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": exp}
    h = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    p = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{h}.{p}."


def make_hs256_token(user_id: str, role: str = "admin", ttl_seconds: int = 3600, secret: str | None = None) -> str:
    """Generate an HS256-signed JWT using `JWT_SECRET`."""
    secret = secret or os.getenv("JWT_SECRET")
    if not secret:
        raise SystemExit("JWT_SECRET env var not set")
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"user_id": user_id, "role": role, "exp": int(time.time()) + int(ttl_seconds)}
    header_b64 = b64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), signing_input, sha256).digest()
    return f"{header_b64}.{payload_b64}.{b64url(sig)}"


def main():
    parser = argparse.ArgumentParser(description="Generate a JWT for News-Ai server")
    parser.add_argument("user_id", nargs="?", default=os.environ.get("NEWSAI_USER_ID", "demo"), help="User ID to embed in the token")
    parser.add_argument("role", nargs="?", default=os.environ.get("NEWSAI_ROLE", "admin"), choices=["user", "admin", "editor"], help="Role to embed in the token")
    parser.add_argument("--ttl", type=int, default=int(os.environ.get("NEWSAI_TTL", 3600)), help="Token TTL in seconds")
    parser.add_argument("--print-curl", action="store_true", help="Print a curl example with the Authorization header")
    args = parser.parse_args()

    token = make_hs256_token(args.user_id, role=args.role, ttl_seconds=args.ttl)

    print(token)
    if args.print_curl:
        print(f"\nExample: curl -H \"Authorization: Bearer {token}\" http://localhost:8000/api/health")


if __name__ == "__main__":
    main()
