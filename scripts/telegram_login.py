import os
import argparse
import asyncio

async def main():
    try:
        from telethon import TelegramClient
    except Exception:
        raise SystemExit("telethon not installed")
    api_id = int(os.getenv("TELEGRAM_API_ID") or 0)
    api_hash = os.getenv("TELEGRAM_API_HASH") or ""
    parser = argparse.ArgumentParser()
    parser.add_argument("--phone", required=False)
    parser.add_argument("--code", required=False)
    parser.add_argument("--password", required=False)
    parser.add_argument("--force-sms", action="store_true")
    args = parser.parse_args()
    phone = args.phone or os.getenv("TELEGRAM_PHONE") or None
    code = args.code or os.getenv("TELEGRAM_CODE") or None
    password = args.password or os.getenv("TELEGRAM_PASSWORD") or None
    if not api_id or not api_hash:
        raise SystemExit("Set TELEGRAM_API_ID and TELEGRAM_API_HASH")
    client = TelegramClient(".telegram_session", api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        if not phone:
            raise SystemExit("Provide --phone or TELEGRAM_PHONE")
        if not code:
            await client.send_code_request(phone, force_sms=bool(args.force_sms))
            print("code_sent")
            code = input("Enter code: ").strip()
        try:
            await client.sign_in(phone=phone, code=code)
        except Exception:
            if not password:
                password = input("Enter 2FA password (if set): ").strip()
            await client.sign_in(password=password)
    me = await client.get_me()
    print("logged_in", getattr(me, "id", None))
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
