#!/usr/bin/env python3
"""
migrate_users_from_old.py

Migrate old `exanic.users` into `ExanicNew.users` with desired schema:
  { user_id: int, amount: int/float, count: int, name: str, username: str }

Behavior:
- Prefer matching by user_id; fall back to username (case-insensitive).
- Preserve existing created_at when present; otherwise set it.
- Update/overwrite fields: username (source casing), amount, count, name.
- Ensures indexes on target: user_id (unique+sparse), username (unique+sparse).

Usage (defaults assume local Mongo):
  python migrate_users_from_old.py
  python migrate_users_from_old.py --old-db exanic --new-db ExanicNew
  python migrate_users_from_old.py --dry-run
"""

import argparse
import re
from datetime import datetime, UTC
from typing import Any, Optional

from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# ---------- helpers ----------

def to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None

def to_float_or_int(x: Any):
    if x is None:
        return 0
    try:
        f = float(x)
        # return int if exact integer
        return int(f) if f.is_integer() else f
    except Exception:
        return 0

def get_ci(col, username: str):
    """Find a document by username, case-insensitive."""
    # Uses regex ^username$ with 'i' option
    return col.find_one({"username": {"$regex": f"^{re.escape(username)}$", "$options": "i"}})

def ensure_indexes(target_col):
    try:
        target_col.create_index([("user_id", ASCENDING)], name="user_id_unique", unique=True, sparse=True)
    except Exception as e:
        print("[index] user_id:", e)
    try:
        target_col.create_index([("username", ASCENDING)], name="username_unique", unique=True, sparse=True)
    except Exception as e:
        print("[index] username:", e)

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old-uri", default="mongodb://127.0.0.1:27017")
    ap.add_argument("--old-db",  default="exanic")
    ap.add_argument("--old-col", default="users")
    ap.add_argument("--new-uri", default="mongodb://127.0.0.1:27017")
    ap.add_argument("--new-db",  default="ExanicNew")
    ap.add_argument("--new-col", default="users")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    src_cli = MongoClient(args.old_uri)
    dst_cli = MongoClient(args.new_uri)

    src = src_cli[args.old_db][args.old_col]
    dst = dst_cli[args.new_db][args.new_col]

    ensure_indexes(dst)

    total = migrated = skipped = conflicts = 0

    print(f"Source: {args.old_uri} / {args.old_db}.{args.old_col}")
    print(f"Target: {args.new_uri} / {args.new_db}.{args.new_col}")
    print(f"Dry-run: {args.dry_run}\n")

    for doc in src.find({}):
        total += 1

        # Pull fields from old doc; adjust keys if your old schema differs
        user_id  = to_int(doc.get("user_id"))
        username = doc.get("username") or ""
        name     = doc.get("name") or None
        amount   = to_float_or_int(doc.get("amount"))
        count    = to_int(doc.get("count")) or 0

        if not user_id and not username:
            skipped += 1
            continue

        now = datetime.now(UTC)
        update = {
            "$set": {
                "username": username,   # keep source casing
                "amount": amount,
                "count": count,
                "name": name,
                "updated_at": now,
            },
            "$setOnInsert": {
                "created_at": now,
            }
        }

        try:
            if args.dry_run:
                migrated += 1
                continue

            if user_id is not None:
                # Primary upsert by user_id
                res = dst.update_one({"user_id": user_id}, update, upsert=True)

                # If a doc exists only by username (ci), link it to user_id to avoid duplicates
                if username:
                    existing_ci = get_ci(dst, username)
                    if existing_ci and existing_ci.get("user_id") is None and (res.upserted_id is None or existing_ci["_id"] != res.upserted_id):
                        try:
                            dst.update_one({"_id": existing_ci["_id"]}, {"$set": {"user_id": user_id}})
                        except DuplicateKeyError:
                            # Another doc already has that user_id; ignore
                            pass

            else:
                # No user_id: upsert by username (ci)
                # Prefer to update existing ci doc to preserve created_at
                ex = get_ci(dst, username)
                if ex:
                    dst.update_one({"_id": ex["_id"]}, update)
                else:
                    dst.update_one({"username": username}, update, upsert=True)

            migrated += 1

        except DuplicateKeyError as e:
            conflicts += 1
            print("[dup]", e)
        except Exception as e:
            conflicts += 1
            print("[err]", e)

    print("\n=== Users Migration Report ===")
    print(f"Total read:        {total}")
    print(f"Migrated/upserted: {migrated}")
    print(f"Skipped:           {skipped}")
    print(f"Conflicts/Errors:  {conflicts}")

    src_cli.close()
    dst_cli.close()


if __name__ == "__main__":
    main()
