from typing import Optional
from db import COL_ESCROWERS
from config import OWNER_ID


async def is_owner(user_id: int) -> bool:
    """Check if a user is one of the bot owners."""
    try:
        # Works for both int or list types
        return user_id in OWNER_ID
    except TypeError:
        return user_id == OWNER_ID


async def is_escrower(user_id: int) -> bool:
    """Check if a user exists in the escrowers database."""
    doc = await COL_ESCROWERS.find_one({"user_id": user_id})
    return bool(doc)


async def is_admin_or_owner(user_id: int) -> bool:
    """Check if user is either owner or registered escrower."""
    return (await is_owner(user_id)) or (await is_escrower(user_id))
