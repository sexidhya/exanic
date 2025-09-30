# gday.py (counts-backed)
from telethon import events
from datetime import datetime, timedelta
from db import COL_ESCROWERS, COL_COUNTS

async def is_escrower(user_id: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": user_id}))

def ist_bucket_utc() -> datetime:
    now_utc = datetime.utcnow()
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    start_ist = datetime(now_ist.year, now_ist.month, now_ist.day)
    return start_ist - timedelta(hours=5, minutes=30)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/gday(?:@[\w_]+)?$"))
    async def gday_handler(event):
        if not await is_escrower(event.sender_id):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        day = ist_bucket_utc()
        cursor = COL_COUNTS.find({"scope": "group_daily", "date_utc": day})
        rows = [doc async for doc in cursor]

        if not rows:
            await event.reply("📊 Group Summary (Today, IST)\n➥ No closed deals today.")
            return

        lines, t_deals, t_fees, t_main = ["📊 Group Summary (Today, IST)"], 0, 0.0, 0.0
        for r in rows:
            gid = int(r["group_id"])
            deals = int(r.get("deals", 0)); fees = float(r.get("fees", 0.0)); main = float(r.get("volume_main", 0.0))
            t_deals += deals; t_fees += fees; t_main += main
            lines.append(f"→ Deals: {deals} | Fees: {fees:.2f}$ | Volume: {main:.2f}$")

        lines.append("")
        lines.append(f"🏁 Total Today: Deals {t_deals} | Fees {t_fees:.2f}$ | Volume {t_main:.2f}$")
        await event.reply("\n".join(lines))
