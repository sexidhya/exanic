# eday.py
from telethon import events
from db import COL_DEALS, COL_ESCROWERS
from datetime import datetime, timedelta

# ---------- helpers ----------
async def is_escrower(user_id: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": user_id}))

def ist_utc_window_for_today() -> tuple[datetime, datetime]:
    """
    Returns (start_utc, end_utc) for 'today' in Asia/Kolkata (UTC+05:30),
    where created_at is stored in UTC.
    """
    now_utc = datetime.utcnow()
    # compute current IST date by adding 5h30m, then take midnight, then convert back to UTC
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    start_ist_midnight = datetime(now_ist.year, now_ist.month, now_ist.day)  # naive IST midnight
    start_utc = start_ist_midnight - timedelta(hours=5, minutes=30)
    end_utc = start_utc + timedelta(days=1)
    return start_utc, end_utc

async def resolve_user_id(client, token: str | None, fallback_id: int) -> int:
    if not token:
        return fallback_id
    token = token.strip().lstrip("@").replace("https://t.me/", "").replace("t.me/", "").split("?")[0]
    try:
        entity = await client.get_entity(int(token) if token.isdigit() else token)
        return entity.id
    except Exception:
        return fallback_id

# ---------- register ----------
def register(client):
    @client.on(events.NewMessage(pattern=r"^/eday(?:\s+(\S+))?$"))
    async def eday_handler(event):
        # only escrowers can use
        if not await is_escrower(event.sender_id):
            await event.reply("⛔ You are not authorized to use this command.")
            return

        # resolve target escrower (default: caller)
        arg = event.pattern_match.group(1)
        escrower_id = await resolve_user_id(event.client, arg, event.sender_id)

        start_utc, end_utc = ist_utc_window_for_today()

        pipeline = [
            {"$match": {
                "escrower_id": escrower_id,
                "status": "closed",
                "created_at": {"$gte": start_utc, "$lt": end_utc},
            }},
            {"$group": {
                "_id": None,
                "deals": {"$sum": 1},
                "fees": {"$sum": {"$ifNull": ["$fee", 0.0]}},
                "main_sum": {"$sum": {"$ifNull": ["$main_amount", 0.0]}}
            }},
        ]

        agg = [d async for d in COL_DEALS.aggregate(pipeline)]
        deals_today = int(agg[0]["deals"]) if agg else 0
        fees_today = float(agg[0]["fees"]) if agg else 0.0
        main_sum_today = float(agg[0]["main_sum"]) if agg else 0.0

        await event.reply(
            "📅 Escrower Summary (Today, IST)\n"
            f"➥ Escrower ID: {escrower_id}\n"
            f"➥ Deals Closed: {deals_today}\n"
            f"➥ Fees Earned: {fees_today:.2f}$\n"
            f"➥ Main Volume: {main_sum_today:.2f}$"
        )
