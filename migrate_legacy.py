# migrate_legacy.py
import json
import asyncio
from datetime import datetime, UTC
from pymongo.errors import DuplicateKeyError
from db import COL_USERS  # your Motor (async) collection

def _coerce_user_id(v):
    if isinstance(v, dict) and "$numberLong" in v:
        return int(v["$numberLong"])
    return int(v)

async def upsert_user(uid: int, name, username: str | None, legacy_amount: float, legacy_count: int):
    base_update = {
        "$setOnInsert": {"created_at": datetime.now(UTC)},
        "$set": {
            "legacy_volume": legacy_amount,
            "legacy_count":  legacy_count,
            "name": name,
        },
    }

    # try with username if present
    if username:
        try:
            update_with_username = {**base_update, "$set": {**base_update["$set"], "username": username}}
            await COL_USERS.update_one({"user_id": uid}, update_with_username, upsert=True)
            return "OK"
        except DuplicateKeyError:
            # username belongs to another doc; retry without setting username
            pass

    # fallback: ensure username field is absent so unique index doesn’t collide
    update_without_username = {**base_update, "$unset": {"username": ""}}
    await COL_USERS.update_one({"user_id": uid}, update_without_username, upsert=True)
    return "OK_NO_USERNAME"

async def migrate_legacy_from_json(path: str):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ok, ok_no_user, merged = 0, 0, 0
    dup_report = {}  # optional: collect collisions for your review

    for entry in data:
        uid = _coerce_user_id(entry.get("user_id"))
        legacy_amount = float(entry.get("amount", 0.0) or 0.0)
        legacy_count  = int(entry.get("count", 0) or 0)
        name = entry.get("name")
        raw_username = entry.get("username")
        username = (raw_username or "").strip().lstrip("@").lower() or None

        try:
            res = await upsert_user(uid, name, username, legacy_amount, legacy_count)
            if res == "OK":
                ok += 1
            else:
                ok_no_user += 1
        except DuplicateKeyError as e:
            # If you ever hit here, record and continue
            dup_report.setdefault(username or "", []).append(uid)

    print(f"✅ Imported legacy data. Success: {ok}, success w/o username: {ok_no_user}")
    if dup_report:
        print("⚠️ Colliding usernames (belong to another user_id):")
        for uname, uids in dup_report.items():
            print(f"  {uname}: {uids}")

if __name__ == "__main__":
    asyncio.run(migrate_legacy_from_json("Exanic.Users.json"))
