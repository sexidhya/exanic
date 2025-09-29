from typing import Any, Dict, List, Optional, Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase

CONSIDERED_STATUSES = ["closed"]

async def _deal_volumes(db: AsyncIOMotorDatabase) -> Dict[int, float]:
    """
    Aggregate current closed deal volumes per user_id (buyer + seller).
    We join deals -> users collection to resolve user_id.
    """
    # Buyer side
    buyer_pipeline = [
        {"$match": {"status": {"$in": CONSIDERED_STATUSES}}},
        {"$lookup": {
            "from": "users",
            "localField": "buyer_username",
            "foreignField": "username",
            "as": "buyer_user"
        }},
        {"$unwind": {"path": "$buyer_user", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "user_id": "$buyer_user.user_id",
            "amount": {"$ifNull": ["$amount", 0]}
        }},
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {"_id": "$user_id", "total_volume": {"$sum": "$amount"}}},
    ]

    # Seller side
    seller_pipeline = [
        {"$match": {"status": {"$in": CONSIDERED_STATUSES}}},
        {"$lookup": {
            "from": "users",
            "localField": "seller_username",
            "foreignField": "username",
            "as": "seller_user"
        }},
        {"$unwind": {"path": "$seller_user", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "user_id": "$seller_user.user_id",
            "amount": {"$ifNull": ["$amount", 0]}
        }},
        {"$match": {"user_id": {"$ne": None}}},
        {"$group": {"_id": "$user_id", "total_volume": {"$sum": "$amount"}}},
    ]

    buyer_cursor = db["deals"].aggregate(buyer_pipeline)
    seller_cursor = db["deals"].aggregate(seller_pipeline)

    totals: Dict[int, float] = {}
    for cursor in (buyer_cursor, seller_cursor):
        async for doc in cursor:
            uid = int(doc["_id"])
            amt = float(doc["total_volume"])
            totals[uid] = totals.get(uid, 0.0) + amt
    return totals

async def _legacy_volumes(db: AsyncIOMotorDatabase) -> Dict[int, float]:
    """
    Legacy totals (from old JSON import).
    """
    cursor = db["users"].find({"legacy_volume": {"$gt": 0}}, {"user_id": 1, "legacy_volume": 1})
    out: Dict[int, float] = {}
    async for u in cursor:
        uid = int(u["user_id"])
        out[uid] = out.get(uid, 0.0) + float(u.get("legacy_volume", 0.0))
    return out

async def _merged_volumes(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    """
    Merge current + legacy volumes keyed by user_id, attach display names.
    """
    current = await _deal_volumes(db)
    legacy = await _legacy_volumes(db)

    totals: Dict[int, float] = {}
    for uid, vol in current.items():
        totals[uid] = totals.get(uid, 0.0) + vol
    for uid, vol in legacy.items():
        totals[uid] = totals.get(uid, 0.0) + vol

    # fetch display names
    users_map: Dict[int, str] = {}
    cursor = db["users"].find({"user_id": {"$in": list(totals.keys())}}, {"user_id": 1, "name": 1})
    async for u in cursor:
        uid = int(u["user_id"])
        display_name = u.get("name")
        users_map[uid] = display_name.strip() if display_name else str(uid)

    merged = [
        {"user_id": uid, "name": users_map.get(uid, str(uid)), "total_volume": vol}
        for uid, vol in totals.items()
    ]
    merged.sort(key=lambda x: (-x["total_volume"], x["user_id"]))
    return merged

async def get_top20_by_volume(db: AsyncIOMotorDatabase) -> List[Dict[str, Any]]:
    vols = await _merged_volumes(db)
    return vols[:20]

async def get_user_rank_by_volume(db: AsyncIOMotorDatabase, user_id: int) -> Optional[Tuple[int, float]]:
    vols = await _merged_volumes(db)
    rank = 0
    prev = None
    for doc in vols:
        v = float(doc.get("total_volume") or 0.0)
        if prev is None or v < prev:
            rank += 1
            prev = v
        if doc.get("user_id") == user_id:
            return rank, v
    return None
