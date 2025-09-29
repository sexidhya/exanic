import asyncio
from pymongo import ASCENDING
from db import COL_USERS  # reuses your db.py connection & config


async def fix_users_index():
    try:
        # Drop the old non-sparse unique index if it exists
        await COL_USERS.drop_index("user_id_1")
        print("✅ Dropped old index 'user_id_1'")
    except Exception as e:
        print("ℹ️ Could not drop index 'user_id_1':", e)

    # Create new sparse unique index
    await COL_USERS.create_index(
        [("user_id", ASCENDING)],
        name="user_id_unique",
        unique=True,
        sparse=True
    )
    print("✅ Created sparse unique index 'user_id_unique' on user_id")


if __name__ == "__main__":
    asyncio.run(fix_users_index())
