from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase


async def global_stats(db: AsyncIOMotorDatabase) -> Tuple[float, int, float]:
    """Return (total_volume, total_count, avg)."""
    pipeline = [
        {
            "$group": {
                "_id": None,
                "sum": {"$sum": {"$ifNull": ["$amount", 0]}},
                "count": {"$sum": 1}
            }
        },
    ]
    agg = [d async for d in db["deals"].aggregate(pipeline)]
    if not agg:
        return 0.0, 0, 0.0

    total = 531713+float(agg[0].get("sum", 0.0))
    cnt = 797+int(agg[0].get("count", 0))
    avg = round(total / cnt, 2) if cnt else 0.0

    return round(total, 2), cnt, avg
