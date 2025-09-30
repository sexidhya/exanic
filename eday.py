# eday.py (counts-backed)
from telethon import events
from datetime import datetime, timedelta
from db import COL_ESCROWERS, COL_COUNTS, COL_USERS

async def is_escrower(user_id: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": user_id}))

def ist_bucket_utc() -> datetime:
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    start_ist = datetime(now_ist.year, now_ist.month, now_ist.day)
    return start_ist - timedelta(hours=5, minutes=30)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/eday(?:@[\w_]+)?$"))
    async def eday_handler(event):
        uid = event.sender_id
        if not await is_escrower(uid):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        # resolve escrower name from users collection
        user_doc = await COL_USERS.find_one({"user_id": uid}, {"name": 1})
        esc_name = user_doc.get("name") if user_doc else str(uid)

        day = ist_bucket_utc()
        doc = await COL_COUNTS.find_one(
            {"scope": "escrower_daily", "escrower_id": uid, "date_utc": day}
        ) or {}

        deals = int(doc.get("deals", 0))
        fees  = float(doc.get("fees", 0.0))
        main  = float(doc.get("volume_main", 0.0))

        await event.reply(
            "📅 Escrower Summary (Today, IST)\n"
            f"➥ Escrower: {esc_name}\n"
            f"➥ Deals Closed: {deals}\n"
            f"➥ Fees Earned: {fees:.2f}$\n"
            f"➥ Today Volume: {main:.2f}$"
        )
