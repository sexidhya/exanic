from typing import Any, Dict, Optional
from motor.motor_asyncio import AsyncIOMotorDatabase
from config import FOOTER_INFO_DATE
from rank import get_user_rank_by_volume
from utils.format import compact_usd

CONSIDERED_STATUSES = [
    "pending", "active", "completed", "released",
    "disputed", "cancelled", "shifted"
]


async def _user_deal_stats(db: AsyncIOMotorDatabase, uname: str) -> Dict[str, Any]:
    pipeline = [
        {
            "$match": {
                "status": {"$in": CONSIDERED_STATUSES},
                "$or": [
                    {"buyer_username": uname},
                    {"seller_username": uname}
                ]
            }
        },
        {
            "$group": {
                "_id": None,
                "count": {"$sum": 1},
                "total_volume": {"$sum": {"$ifNull": ["$amount", 0]}},
            }
        },
    ]
    agg = [doc async for doc in db["deals"].aggregate(pipeline)]
    if not agg:
        return {"count": 0, "total_volume": 0.0}
    d = agg[0]
    return {
        "count": int(d.get("count", 0)),
        "total_volume": float(d.get("total_volume", 0.0)),
    }
FOOTER_INFO_DATE1 = "💡 Data Recorded from 29/09/2025 20:00 IST"

async def build_info_card(
    db: AsyncIOMotorDatabase,
    *,
    user_id: Optional[int] = None,
    username: Optional[str] = None,
) -> str:
    """
    Build an info card for the given user.
    Handles:
      - classic usernames
      - collectible usernames (if stored in db)
      - users with no username at all
    """

    # Try to find the user in DB
    user = None
    if user_id is not None:
        user = await db["users"].find_one({"user_id": user_id})
    if not user and username:
        user = await db["users"].find_one({"username": username.lstrip("@").lower()})

    uid = (user or {}).get("user_id", user_id or 0)

    # prefer passed-in username, else DB value
    raw_uname = username or (user or {}).get("username")
    uname = raw_uname.lstrip("@").lower() if raw_uname else None

    # Stats + rank only if we have a username
    if uname:
        stats = await _user_deal_stats(db, uname)
        r = await get_user_rank_by_volume(db, uname)
        rank_str = str(r[0]) if r else "0"
        amount_str = compact_usd(stats["total_volume"])
        total_escrows = stats["count"]
        username_line = f"@{uname}"
    else:
        stats = {"count": 0, "total_volume": 0.0}
        rank_str = "0"
        amount_str = compact_usd(0)
        total_escrows = 0
        username_line = "(no username)"

    lines = [
        "User Info:",
        "",
        f"User ID: {uid}",
        f"Username: {username_line}",
        f"Total Escrows: {total_escrows}",
        f"Escrowed Amount: {amount_str}",
        f"Rank: {rank_str}",
        "",
        FOOTER_INFO_DATE1,
    ]
    return "\n".join(lines)
