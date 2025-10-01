# stats_counters.py
from datetime import datetime, timezone
UTC = timezone.utc
from db import COL_COUNTS, COL_DEALS

def _ist_bucket_utc(dt_utc: datetime) -> datetime:
    dt_ist = dt_utc + timedelta(hours=5, minutes=30)
    return datetime(dt_ist.year, dt_ist.month, dt_ist.day) - timedelta(hours=5, minutes=30)

async def increment_counters_for_closed(deal: dict) -> None:
    """
    Count this closed deal exactly once.
    Marks deal.stats_accounted = True to avoid double counting.
    """
    if not deal:
        return

    # 1) guard: only closed deals
    if deal.get("status") != "closed":
        return

    # 2) idempotency flag
    res = await COL_DEALS.update_one(
        {"_id": deal["_id"], "stats_accounted": {"$ne": True}},
        {"$set": {"stats_accounted": True}}
    )
    if res.modified_count == 0:
        return  # already counted earlier

    main = float(deal.get("main_amount") or 0.0)
    fee  = float(deal.get("fee") or 0.0)
    escrower_id = int(deal.get("escrower_id") or 0)
    group_id    = int(deal.get("form_chat_id") or 0)
    closed_at   = deal.get("closed_at")
    if not isinstance(closed_at, datetime):
        closed_at = datetime.now(UTC)
    day_bucket  = _ist_bucket_utc(closed_at)

    # 3) global lifetime
    await COL_COUNTS.update_one(
        {"scope": "global"},
        {"$inc": {"deals": 1, "volume_main": main, "fees": fee}},
        upsert=True
    )

    # 4) global daily
    await COL_COUNTS.update_one(
        {"scope": "daily", "date_utc": day_bucket},
        {"$inc": {"deals": 1, "volume_main": main, "fees": fee}},
        upsert=True
    )

    # 5) per group daily
    if group_id:
        await COL_COUNTS.update_one(
            {"scope": "group_daily", "group_id": group_id, "date_utc": day_bucket},
            {"$inc": {"deals": 1, "volume_main": main, "fees": fee}},
            upsert=True
        )

    # 6) per escrower daily
    if escrower_id:
        await COL_COUNTS.update_one(
            {"scope": "escrower_daily", "escrower_id": escrower_id, "date_utc": day_bucket},
            {"$inc": {"deals": 1, "volume_main": main, "fees": fee}},
            upsert=True
        )
