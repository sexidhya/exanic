from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase

# Baseline values
BASE_TOTAL = 531_713.64
BASE_COUNT = 797

from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase
from db import COL_DEALS

async def global_stats(db: AsyncIOMotorDatabase) -> Tuple[float, int, float]:
    """Return (total_volume, total_count, avg) for CLOSED deals only (main_amount)."""
    pipeline = [
        {"$match": {"status": "closed"}},
        {"$group": {
            "_id": None,
            "sum": {"$sum": {"$ifNull": ["$main_amount", 0]}},
            "count": {"$sum": 1}
        }},
    ]
    agg = [d async for d in COL_DEALS.aggregate(pipeline)]
    if not agg:
        return 0.0, 0, 0.0

    total = float(agg[0].get("sum", 0.0))
    cnt = int(agg[0].get("count", 0))
    avg = round(total / cnt, 2) if cnt else 0.0
    return round(total, 2), cnt, avg


