# gstats.py
from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase
from db import COL_DEALS, COL_COUNTS

# 🔢 Baseline values (seeded totals you want to start from)
BASE_TOTAL = 531_713.64  # USD
BASE_COUNT = 797         # deals

async def _from_counts() -> Tuple[float, int]:
    """
    Read lifetime totals from counts (scope='global').
    Returns (volume_main, deals). If not present, returns (0.0, 0).
    """
    doc = await COL_COUNTS.find_one({"scope": "global"}) or {}
    vol = float(doc.get("volume_main", 0.0))
    cnt = int(doc.get("deals", 0))
    return vol, cnt

async def _from_deals() -> Tuple[float, int]:
    """
    Fallback: aggregate from deals (closed only, main_amount).
    Returns (volume_main, deals).
    """
    pipeline = [
        {"$match": {"status": "closed"}},
        {"$group": {
            "_id": None,
            "sum": {"$sum": {"$ifNull": ["$main_amount", 0]}},
            "count": {"$sum": 1},
        }},
    ]
    agg = [d async for d in COL_DEALS.aggregate(pipeline)]
    if not agg:
        return 0.0, 0
    return float(agg[0].get("sum", 0.0)), int(agg[0].get("count", 0))

async def global_stats(db: AsyncIOMotorDatabase) -> Tuple[float, int, float]:
    """
    Return (total_volume, total_count, avg) for CLOSED deals (main_amount only),
    **including** the configured baselines.
    - Primary source: counts (scope='global')
    - Fallback: aggregate from deals when counts is empty
    """
    vol, cnt = await _from_counts()
    if vol == 0.0 and cnt == 0:
        # counts not populated yet → fallback to aggregation
        vol, cnt = await _from_deals()

    total = vol + BASE_TOTAL
    total_count = cnt + BASE_COUNT
    avg = round(total / total_count, 2) if total_count else 0.0
    return round(total, 2), total_count, avg
