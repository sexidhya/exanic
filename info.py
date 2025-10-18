# info.py
from typing import Any, Dict, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from rank import get_user_rank_by_volume
from utils.format import compact_usd

from config import FOOTER_INFO_DATE

CONSIDERED_STATUSES = ["closed"]

async def _user_deal_stats_current(db: AsyncIOMotorDatabase, user_id: int) -> Dict[str, Any]:
    """
    Stats from deals collection for given user_id (buyer or seller).
    """
    pipeline = [
        {"$match": {"status": {"$in": CONSIDERED_STATUSES}}},
        {"$lookup": {
            "from": "users",
            "localField": "buyer_username",
            "foreignField": "username",
            "as": "buyer_user"
        }},
        {"$unwind": {"path": "$buyer_user", "preserveNullAndEmptyArrays": True}},
        {"$lookup": {
            "from": "users",
            "localField": "seller_username",
            "foreignField": "username",
            "as": "seller_user"
        }},
        {"$unwind": {"path": "$seller_user", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "buyer_id": "$buyer_user.user_id",
            "seller_id": "$seller_user.user_id",
            "amount": {"$ifNull": ["$amount", 0]}
        }},
        {"$match": {"$or": [{"buyer_id": user_id}, {"seller_id": user_id}]}},
        {"$group": {
            "_id": None,
            "count": {"$sum": 1},
            "total_volume": {"$sum": "$amount"},
        }},
    ]
    agg = [doc async for doc in db["deals"].aggregate(pipeline)]
    if not agg:
        return {"count": 0, "total_volume": 0.0}
    d = agg[0]
    return {"count": int(d.get("count", 0)), "total_volume": float(d.get("total_volume", 0.0))}

async def build_info_card(db: AsyncIOMotorDatabase, *, user_id: int) -> str:
    """
    Build an info card purely from user_id.
    Totals = current (from deals) + legacy (from users collection).
    """
    user = await db["users"].find_one({"user_id": user_id})
    if not user:
        return f"User Info:\n\nUser ID: {user_id}\n❌ No data found."

    name = user.get("name") or str(user_id)

    # current stats (from deals)
    cur = await _user_deal_stats_current(db, user_id)

    # legacy stats (from users collection)
    legacy_count = int(user.get("legacy_count", 0) or 0)
    legacy_volume = float(user.get("legacy_volume", 0.0) or 0.0)

    total_count = cur["count"] + legacy_count
    total_volume = cur["total_volume"] + legacy_volume

    # rank by merged volume
    rank_info = await get_user_rank_by_volume(db, user_id)
    rank_str = str(rank_info[0]) if rank_info else "0"

    amount_str = compact_usd(total_volume)
    lines = [
        "**✅User Info:**",
        "",
        f"**User ID:** {user_id}",
        f"**Name:** {name}",
        f"**Total Escrows:** {total_count}",
        f"**Escrowed Amount:** {amount_str}",
        f"**Rank:** {rank_str}",
        "",
        FOOTER_INFO_DATE,
    ]
    return "\n".join(lines)
