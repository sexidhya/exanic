from typing import Dict
from motor.motor_asyncio import AsyncIOMotorDatabase

async def escrower_holdings(db: AsyncIOMotorDatabase) -> Dict[str, float]:
    pipeline = [
        {"$match": {"status": {"$in": ["pending", "active"]}}},  # <— only ongoing
        {
            "$group": {
                "_id": {
                    "escrower_id": "$escrower_id",
                    "escrower_name": "$escrower_name",
                },
                "hold": {"$sum": {"$ifNull": ["$remaining", 0]}},
            }
        },
        {"$sort": {"_id.escrower_name": 1}},
    ]
    res: Dict[str, float] = {}
    async for d in db["deals"].aggregate(pipeline):
        key = f"{d['_id']['escrower_name']} ({d['_id']['escrower_id']})"
        res[key] = float(d.get("hold") or 0.0)
    return res
