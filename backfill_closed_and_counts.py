# backfill_closed_and_counts.py
import asyncio
from datetime import datetime, UTC
from db import COL_DEALS
from stats_counters import increment_counters_for_closed

async def run():
    n = 0
    async for d in COL_DEALS.find({"status": "closed"}):
        if "closed_at" not in d:
            await COL_DEALS.update_one({"_id": d["_id"]}, {"$set": {"closed_at": datetime.now(UTC)}})
        await increment_counters_for_closed(d)
        n += 1
    print(f"✅ backfilled {n} closed deals into counts")

if __name__ == "__main__":
    asyncio.run(run())
