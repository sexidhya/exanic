import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from config import MONGO_URI, DB_NAME

# collections you normally use
COLLECTIONS = ["users", "deals", "escrowers", "logs"]

async def clean_collection(db, name: str):
    """Delete all documents from one collection."""
    if name not in db.list_collection_names:
        print(f"⚠️ Collection {name} not found in DB.")
        return
    res = await db[name].delete_many({})
    print(f"🗑 Cleared {res.deleted_count} documents from {name}")

async def clean_db(db):
    """Drop the entire database."""
    await db.client.drop_database(DB_NAME)
    print(f"💥 Dropped entire database {DB_NAME}")

async def main():
    client = AsyncIOMotorClient(MONGO_URI)
    db = client[DB_NAME]

    print("=== DB Cleaner ===")
    print("Options:")
    print("1. Clean a single collection")
    print("2. Clean all known collections (users, deals, escrowers, logs)")
    print("3. Drop whole database")
    choice = input("Enter choice (1/2/3): ").strip()

    if choice == "1":
        name = input(f"Enter collection name ({', '.join(COLLECTIONS)}): ").strip()
        await clean_collection(db, name)
    elif choice == "2":
        for name in COLLECTIONS:
            await clean_collection(db, name)
    elif choice == "3":
        confirm = input(f"⚠️ Really drop entire DB {DB_NAME}? (yes/no): ").strip().lower()
        if confirm == "yes":
            await clean_db(db)
        else:
            print("❌ Cancelled.")
    else:
        print("❌ Invalid choice.")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
