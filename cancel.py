from telethon import events
from db import COL_DEALS, COL_ESCROWERS
async def is_escrower(user_id: int) -> bool:
    doc = await COL_ESCROWERS.find_one({"user_id": user_id})
    return bool(doc)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/cancel\s+(\S+)"))
    async def cancel_deal(event):
        # ✅ Restriction check
        if not await is_escrower(event.sender_id):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        deal_id = event.pattern_match.group(1).strip().upper()
        deal = await COL_DEALS.find_one({"deal_id": deal_id})

        if not deal:
            await event.reply("❌ Deal not found.")
            return

        if deal.get("status") != "active":
            await event.reply(f"⚠️ Deal {deal_id} is already {deal.get('status')}.")
            return

        # ✅ Cancel the deal
        await COL_DEALS.update_one(
            {"deal_id": deal_id},
            {"$set": {"status": "cancelled"}}
        )

        await event.reply(f"✅ Deal {deal_id} has been cancelled.")
