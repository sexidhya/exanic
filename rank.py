from typing import Any, Dict, List, Optional, Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase

# ✅ Only include closed deals
CONSIDERED_STATUSES = ["closed"]

async def _user_volumes(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": {"status": {"$in": CONSIDERED_STATUSES}}},
        {"$project": {
            "participants": ["$buyer_username", "$seller_username"],
            "amount": {"$ifNull": ["$amount", 0]},
        }},
        {"$unwind": "$participants"},
        {"$project": {  # normalize usernames
            "u": {"$toLower": "$participants"},
            "amount": 1
        }},
        {"$match": {"u": {"$ne": None, "$ne": ""}}},
        {"$group": {
            "_id": "$u",
            "total_volume": {"$sum": "$amount"},
        }},
        {"$sort": {"total_volume": -1, "_id": 1}},
    ]
    cursor = db["deals"].aggregate(pipeline)
    return [doc async for doc in cursor]

async def get_top20_by_volume(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    vols = await _user_volumes(db)
    return vols[:20]

async def get_user_rank_by_volume(db: AsyncIOMotorDatabase, username: str) -> Optional[Tuple[int, float]]:
    if not username:
        return None
    uname = username.lstrip("@").lower()
    vols = await _user_volumes(db)

    rank = 0
    prev = None
    for doc in vols:
        v = float(doc.get("total_volume") or 0)
        if prev is None or v < prev:
            rank += 1
            prev = v
        if doc.get("_id") == uname:
            return rank, v
    return None
