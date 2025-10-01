# gstats.py
from typing import Tuple
from motor.motor_asyncio import AsyncIOMotorDatabase
from db import read_simple_global

# Baselines (seeded totals you want to start from)
BASE_TOTAL = 531_713.64  # USD
BASE_COUNT = 797         # deals

async def global_stats(db: AsyncIOMotorDatabase) -> Tuple[float, int, float]:
    """
    Return (total_volume, total_count, avg) for CLOSED deals,
    including baselines. Reads from the simple global counter doc.
    """
    vol, cnt = await read_simple_global()

    total = vol + BASE_TOTAL
    total_count = cnt + BASE_COUNT
    avg = round(total / total_count, 2) if total_count else 0.0

    return round(total, 2), total_count, avg
