from typing import Optional
from db import COL_ESCROWERS
from config import OWNER_ID


async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


async def is_escrower(user_id: int) -> bool:
    doc = await COL_ESCROWERS.find_one({"user_id": user_id})
    return bool(doc)


async def is_admin_or_owner(user_id: int) -> bool:
    return (await is_owner(user_id)) or (await is_escrower(user_id))
