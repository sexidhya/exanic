# close_cmd.py
from telethon import events
from datetime import datetime, timezone
UTC = timezone.utc
from pymongo import ReturnDocument
from typing import Union, Any
import re
from html import escape as htmlesc

from db import COL_DEALS, COL_ESCROWERS, read_simple_global, increment_counters_for_closed
from utils.format import mask_name
from config import LOG_CHANNEL_ID

# Baselines (if you still want seeded totals)
BASE_TOTAL = 531_713.64
BASE_COUNT = 797

async def _is_escrower(uid: int) -> bool:
    return bool(await COL_ESCROWERS.find_one({"user_id": uid}))

async def _resolve_log_peer(client, target: Union[int, str]) -> Any:
    if isinstance(target, int):
        return await client.get_entity(target)
    t = str(target).strip()
    if t.startswith("-100") and t.lstrip("-").isdigit():
        return await client.get_entity(int(t))
    if not t.startswith("@"):
        t = "@" + t
    return await client.get_entity(t)

def _safe(s: Any) -> str:
    return htmlesc(str(s or ""))

async def _delete_cmd_msg(event):
    try:
        await event.delete()
    except Exception:
        pass

def register(client):
    @client.on(events.NewMessage(pattern=r"^/close(?:@[\w_]+)?\s+([0-9]+(?:\.[0-9]+)?)$"))
    async def close_cmd(event):
        # Permissions
        if not await _is_escrower(event.sender_id):
            await event.respond("❌ Only escrowers can use /close.")
            await _delete_cmd_msg(event)
            return

        # Must reply to the Escrow Deal card
        if not event.is_reply:
            await event.respond("❌ Reply to the Escrow Deal card with /close <amount>.")
            await _delete_cmd_msg(event)
            return

        # Parse amount
        try:
            close_amount = float(event.pattern_match.group(1))
            if close_amount <= 0:
                raise ValueError
        except Exception:
            await event.respond("❌ Invalid amount.")
            await _delete_cmd_msg(event)
            return

        # The card we're replying to (keep thread)
        card = await event.get_reply_message()

        # Extract deal_id from the card text
        card_text = card.raw_text or ""
        m = re.search(r"\bID\s*-\s*(DL-[A-Z0-9]{6})\b", card_text, flags=re.I)
        if not m:
            await card.reply("❌ Could not detect deal_id from the replied card.")
            await _delete_cmd_msg(event)
            return
        deal_id = m.group(1).upper()

        # Fetch open deal owned by this escrower
        deal = await COL_DEALS.find_one({
            "deal_id": deal_id,
            "escrower_id": event.sender_id,
            "status": {"$in": ["pending", "active"]},
        })
        if not deal:
            await card.reply("❌ Deal not found or already closed.")
            await _delete_cmd_msg(event)
            return

        # Decrement remaining
        updated = await COL_DEALS.find_one_and_update(
            {"_id": deal["_id"]},
            {"$inc": {"remaining": -close_amount}},
            return_document=ReturnDocument.AFTER
        )
        new_remaining = max(0.0, float(updated.get("remaining", 0.0)))

        # Mark closed
        now = datetime.now(UTC)
        await COL_DEALS.update_one(
            {"_id": deal["_id"]},
            {"$set": {
                "status": "closed",
                "closed_at": now,
                "remaining": new_remaining,
                "closed_by": event.sender_id
            }}
        )
        closed_deal = await COL_DEALS.find_one({"_id": deal["_id"]})

        # Update counters (simple + scoped)
        try:
            await increment_counters_for_closed(closed_deal, amount_field="main_amount")
        except Exception as e:
            print(f"⚠️ Closed, but counters update failed: {e!r}")

        # Build announcement (reply to the card to keep thread)
        seller_open = (closed_deal.get("seller_username") or "").lstrip("@")
        buyer_open  = (closed_deal.get("buyer_username")  or "").lstrip("@")
        announce_html = (
            f"<b>✅ Deal <code>{_safe(deal_id)}</code> has been closed!</b>\n"
            f"~ @{_safe(buyer_open)} and @{_safe(seller_open)} are requested to drop the vouch before leaving.\n\n"
            f"<code>Vouch @Exanic for ${close_amount:.2f} deal, safely escrowed.</code>"
        )
        await card.reply(announce_html, parse_mode="html", link_preview=False)

        # Totals for logging (simple global + baseline)
        vol, cnt = await read_simple_global()
        total_worth = vol + BASE_TOTAL
        total_deals = cnt + BASE_COUNT

        escrower = closed_deal.get("escrower_name") or str(closed_deal.get("escrower_id"))
        log_html = (
            "<b>✅ Escrow Deal — Done!</b>\n\n"
            f"<b>ID</b> - <code>{_safe(deal_id)}</code>\n"
            f"<b>Escrower</b> - {_safe(escrower)}\n"
            f"<b>Buyer</b> - {_safe(mask_name(buyer_open))}\n"
            f"<b>Seller</b> - {_safe(mask_name(seller_open))}\n"
            f"<b>Amount</b> - {close_amount:.2f}$\n"
            f"<b>Total Worth</b>: {total_worth:.2f}$\n"
            f"<b>Total Escrows</b>: {total_deals}\n\n"
            "<b>By @Exanic</b>"
        )

        # Send log to channel
        try:
            peer = await _resolve_log_peer(event.client, LOG_CHANNEL_ID)
            await event.client.send_message(peer, log_html, parse_mode="html", link_preview=False)
        except Exception as e:
            print(
                f"⚠️ Deal closed, but logging failed: {e!r}\n"
                f"Please verify LOG_CHANNEL_ID (use -100… or @username) and that the bot is a member."
            )

        # Finally: delete the /close command message
        await _delete_cmd_msg(event)
