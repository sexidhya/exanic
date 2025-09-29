from telethon import events
from db import COL_DEALS, COL_ESCROWERS
from datetime import datetime, timedelta

async def is_escrower(user_id: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": user_id}))

def ist_utc_window_for_today() -> tuple[datetime, datetime]:
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    start_ist_midnight = datetime(now_ist.year, now_ist.month, now_ist.day)
    start_utc = start_ist_midnight - timedelta(hours=5, minutes=30)
    end_utc = start_utc + timedelta(days=1)
    return start_utc, end_utc

async def _chat_label(client, chat_id: int) -> str:
    try:
        ent = await client.get_entity(chat_id)
        title = getattr(ent, "title", None)
        if title:
            return f"{title} ({chat_id})"
    except Exception:
        pass
    return str(chat_id)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/gday$"))
    async def gday_handler(event):
        # only escrowers can use
        if not await is_escrower(event.sender_id):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        start_utc, end_utc = ist_utc_window_for_today()

        # Aggregate across ALL groups by form_chat_id
        pipeline = [
            {"$match": {
                "status": "closed",
                "created_at": {"$gte": start_utc, "$lt": end_utc},
                "form_chat_id": {"$type": "number"}  # ensure present
            }},
            {"$group": {
                "_id": "$form_chat_id",
                "deals": {"$sum": 1},
                "fees": {"$sum": {"$ifNull": ["$fee", 0.0]}},
                "main_sum": {"$sum": {"$ifNull": ["$main_amount", 0.0]}},
            }},
            {"$sort": {"main_sum": -1}},
        ]

        rows = [d async for d in COL_DEALS.aggregate(pipeline)]

        if not rows:
            await event.reply("📊 Group Summary (Today, IST)\n➥ No closed deals today.")
            return

        # Build per-group lines (resolve titles if possible)
        lines = ["📊 Group Summary (Today, IST)"]
        total_deals = 0
        total_fees = 0.0
        total_main = 0.0

        for r in rows:
            chat_id = int(r["_id"])
            deals = int(r["deals"])
            fees = float(r["fees"])
            main_sum = float(r["main_sum"])

            total_deals += deals
            total_fees += fees
            total_main += main_sum

            label = await _chat_label(event.client, chat_id)
            lines.append(
                f"• {label}\n"
                f"   → Deals: {deals} | Fees: {fees:.2f}$ | Volume: {main_sum:.2f}$"
            )

        # Grand total
        lines.append("")
        lines.append(
            f"🏁 Total Today: Deals {total_deals} | Fees {total_fees:.2f}$ | Volume {total_main:.2f}$"
        )

        await event.reply("\n".join(lines))
