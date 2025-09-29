from telethon import events
from db import COL_DEALS
from datetime import datetime

def register(client):
    @client.on(events.NewMessage(pattern=r"^/gday$"))
    async def gday_handler(event):
        if not event.is_group:
            await event.reply("⚠️ This command only works in groups.")
            return

        now = datetime.utcnow()
        start = datetime(now.year, now.month, now.day)

        cursor = COL_DEALS.find({
            "form_chat_id": event.chat_id,   # group-specific deals
            "status": {"$in": ["released", "completed"]},
            "created_at": {"$gte": start, "$lte": now}
        })

        deals_today = 0
        async for _ in cursor:
            deals_today += 1

        await event.reply(
            f"📊 Group Deals Today\n"
            f"➥ Group ID: {event.chat_id}\n"
            f"➥ Deals: {deals_today}"
        )
