from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase

# Baseline values
BASE_TOTAL = 531_713.64
BASE_COUNT = 797

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
        # No deals in DB → just return baseline
        return round(BASE_TOTAL, 2), BASE_COUNT, round(BASE_TOTAL / BASE_COUNT, 2)

    total = float(agg[0].get("sum", 0.0)) + BASE_TOTAL
    cnt = int(agg[0].get("count", 0)) + BASE_COUNT
    avg = round(total / cnt, 2) if cnt else 0.0

    return round(total, 2), cnt, avg
