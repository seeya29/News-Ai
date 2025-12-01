import os
import sys
import traceback
from scripts.dev_jwt import make_hs256_token, make_unsigned_token
from server.app import _decode_jwt_payload


def run():
    os.environ.setdefault("JWT_SECRET", "testsecret")
    os.environ.setdefault("JWT_ALG", "HS256")

    ok = True
    try:
        t = make_hs256_token("smoke_user", role="admin", ttl_seconds=60, secret="testsecret")
        claims = _decode_jwt_payload(t)
        print("HS256_ok", claims.get("user_id"), claims.get("role"))
    except Exception:
        ok = False
        print("HS256_fail")
        traceback.print_exc()

    try:
        t2 = make_unsigned_token("smoke_user2", role="user", ttl_seconds=60)
        _decode_jwt_payload(t2)
        ok = False
        print("NONE_unexpected_ok")
    except Exception:
        print("NONE_blocked")

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    run()
