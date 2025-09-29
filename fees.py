from typing import List
from motor.motor_asyncio import AsyncIOMotorDatabase


async def fees_by_escrower(db: AsyncIOMotorDatabase) -> List[dict]:
    """Sum fees per escrower for CLOSED deals only."""
    pipeline = [
        {"$match": {"status": "closed"}},
        {
            "$group": {
                "_id": {
                    "escrower_id": "$escrower_id",
                    "escrower_name": "$escrower_name"
                },
                "total_fees": {"$sum": {"$ifNull": ["$fee", 0]}},
                "deals": {"$sum": 1}
            }
        },
        {"$sort": {"total_fees": -1}}
    ]
    return [d async for d in db["deals"].aggregate(pipeline)]
