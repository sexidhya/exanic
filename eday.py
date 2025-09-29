from telethon import events
from db import COL_DEALS, COL_ESCROWERS
from datetime import datetime, timedelta

async def is_escrower(user_id: int) -> bool:
    doc = await COL_ESCROWERS.find_one({"user_id": user_id})
    return bool(doc)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/eday(?:\s+(\S+))?"))
    async def eday_handler(event):
        # ✅ Restriction check
        if not await is_escrower(event.sender_id):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        arg = event.pattern_match.group(1)
        escrower_id = event.sender_id  # default = self
        if arg:
            try:
                entity = await event.client.get_entity(arg.lstrip("@"))
                escrower_id = entity.id
            except Exception:
                await event.reply("❌ Could not resolve user.")
                return

        # Today’s range (UTC midnight to now)
        now = datetime.utcnow()
        start = datetime(now.year, now.month, now.day)

        cursor = COL_DEALS.find({
            "escrower_id": escrower_id,
            "status": {"$in": ["released", "completed"]},  # adjust if needed
            "created_at": {"$gte": start, "$lte": now}
        }, {"fee": 1})

        deals_today, fees_today = 0, 0.0
        async for d in cursor:
            deals_today += 1
            fees_today += float(d.get("fee", 0.0))

        await event.reply(
            f"📅 Escrower Deals Today\n"
            f"➥ User ID: {escrower_id}\n"
            f"➥ Deals: {deals_today}\n"
            f"➥ Fees Earned: {fees_today:.2f}$"
        )
