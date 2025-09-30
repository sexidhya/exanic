# close_cmd.py
from telethon import events
from datetime import datetime, UTC
from pymongo import ReturnDocument
from typing import Union, Any
import re

from db import COL_DEALS, COL_ESCROWERS, COL_COUNTS
from stats_counters import increment_counters_for_closed
from utils.format import mask_name  # your existing helper

# === CONFIG ===
# Use the raw -100... chat id OR an @public_username. Works with both.
from config import LOG_CHANNEL_ID  # can be: int -100..., or str "-100...", or str "@channelname"

# Optional baselines (if you use seeded totals)
BASE_TOTAL = 531_713.64  # USD
BASE_COUNT = 797

async def _is_escrower(uid: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": uid}))

async def _resolve_log_peer(client, target: Union[int, str]) -> Any:
    """Resolve LOG_CHANNEL_ID in all common formats."""
    if isinstance(target, int):
        return await client.get_entity(target)
    t = str(target).strip()
    if t.startswith("-100") and t.lstrip("-").isdigit():
        return await client.get_entity(int(t))
    if not t.startswith("@"):
        t = "@" + t
    return await client.get_entity(t)

async def _read_totals_from_counts() -> tuple[float, int]:
    doc = await COL_COUNTS.find_one({"scope": "global"}) or {}
    vol = float(doc.get("volume_main", 0.0))
    cnt = int(doc.get("deals", 0))
    return vol + BASE_TOTAL, cnt + BASE_COUNT

def register(client):
    @client.on(events.NewMessage(pattern=r"^/close(?:@[\w_]+)?\s+([0-9]+(?:\.[0-9]+)?)$"))
    async def close_cmd(event):
        # 1) permissions + input
        if not await _is_escrower(event.sender_id):
            await event.respond("❌ Only escrowers can use /close.")
            return
        if not event.is_reply:
            await event.respond("❌ Reply to the Escrow Deal card with /close <amount>.")
            return

        try:
            close_amount = float(event.pattern_match.group(1))
            if close_amount <= 0:
                raise ValueError
        except Exception:
            await event.respond("❌ Invalid amount.")
            return

        # 2) extract deal_id from replied card
        card = await event.get_reply_message()
        m = re.search(r"\bID\s*-\s*(DL-[A-Z0-9]{6})\b", (card.raw_text or ""), flags=re.I)
        if not m:
            await event.respond("❌ Could not detect deal_id from the replied card.")
            return
        deal_id = m.group(1).upper()

        # 3) fetch open deal for this escrower
        deal = await COL_DEALS.find_one({
            "deal_id": deal_id,
            "escrower_id": event.sender_id,
            "status": {"$in": ["pending", "active"]},
        })
        if not deal:
            await event.respond("❌ Deal not found or already closed.")
            return

        # 4) decrement remaining (separate op to avoid $inc/$set conflict)
        updated = await COL_DEALS.find_one_and_update(
            {"_id": deal["_id"]},
            {"$inc": {"remaining": -close_amount}},
            return_document=ReturnDocument.AFTER
        )
        new_remaining = max(0.0, float(updated.get("remaining", 0.0)))

        # 5) set closed status + closed_at (UTC)
        now = datetime.now(UTC)
        await COL_DEALS.update_one(
            {"_id": deal["_id"]},
            {"$set": {"status": "closed", "closed_at": now, "remaining": new_remaining,
                      "closed_by": event.sender_id}}
        )
        closed_deal = await COL_DEALS.find_one({"_id": deal["_id"]})

        # 6) increment counters ONCE (idempotent). Don't block on errors.
        try:
            await increment_counters_for_closed(closed_deal)
        except Exception as e:
            await event.respond(f"⚠️ Closed, but counters update failed: {e!r}")

        # 7) user-facing announcement
        seller_open = (closed_deal.get("seller_username") or "").lstrip("@")
        buyer_open  = (closed_deal.get("buyer_username")  or "").lstrip("@")
        await event.respond(
            "✅ Deal `{}` has been closed!\n"
            "~ @{} and @{} are requested to drop the vouch before leaving.\n\n"
            "`Vouch @Exanic for ${:.2f} deal, safely escrowed.`".format(
                deal_id, buyer_open, seller_open, close_amount
            )
        )

        # 8) totals from counts (+ baseline)
        total_worth, total_deals = await _read_totals_from_counts()
        escrower = closed_deal.get("escrower_name") or str(closed_deal.get("escrower_id"))
        log_text = (
            "✅ Escrow Deal — Done!\n\n"
            f"ID - {deal_id}\n"
            f"Escrower - {escrower}\n"
            f"Buyer - {mask_name(buyer_open)}\n"
            f"Seller - {mask_name(seller_open)}\n"
            f"Amount - {close_amount:.2f}$\n"
            f"Total Worth: {total_worth:.2f}$\n"
            f"Total Escrows: {total_deals}\n\n"
            "By @Exanic"
        )

        # 9) log: resolve once and send
        try:
            peer = await _resolve_log_peer(event.client, LOG_CHANNEL_ID)
            await event.client.send_message(peer, log_text)
        except Exception as e:
            await event.reply(
                f"⚠️ Deal closed, but logging failed: {e!r}\n"
                f"Please verify LOG_CHANNEL_ID (use -100… or @username) and that the bot is a member."
            )
