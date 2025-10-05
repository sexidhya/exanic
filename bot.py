import asyncio
import re
import traceback
import sys
import random
import string
import unicodedata
from datetime import datetime, timezone
UTC = timezone.utc
from telethon.tl.functions.users import GetFullUserRequest
from telethon import TelegramClient, events
from telethon.tl.custom.message import Message

from config import API_ID, API_HASH, BOT_TOKEN, OWNER_ID, ESCROW_GROUP_IDS, FOOTER_INFO_DATE
from db import db, COL_DEALS, COL_ESCROWERS, ensure_indexes , COL_USERS
from parsing import parse_deal_form
from utils.format import normalize_username
from permissions import is_owner, is_escrower, is_admin_or_owner
from rank import get_top20_by_volume
from info import build_info_card
from holdings import escrower_holdings
from gstats import global_stats
from config import LOG_CHANNEL_ID
from fees import fees_by_escrower
from deal_logic import create_deal_from_form, recalc_amount_fields , compute_fee, _new_deal_id
import re
from datetime import datetime
from telethon import events
from telethon.tl.custom import Message

from db import COL_USERS, COL_DEALS, COL_ESCROWERS




# On Windows, use selector policy for Telethon
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from config import API_ID, API_HASH, BOT_TOKEN, FOOTER_INFO_DATE
from db import ensure_indexes, db
from info import build_info_card
from permissions import is_owner
from holdings import escrower_holdings
from gstats import global_stats
from fees import fees_by_escrower
from utils.format import mask_name

# ------------------------------------------------------------------
# Create the client object, but DO NOT start it here.
# (Starting happens inside main() on the same event loop.)
# ------------------------------------------------------------------
client = TelegramClient("escrow_bot", API_ID, API_HASH)

# bot.py (after you define client)
import dinfo , show , cancel , mkick , eday , gday , close_cmd
close_cmd.register(client)
dinfo.register(client)
show.register(client)
cancel.register(client)
mkick.register(client)
eday.register(client)
gday.register(client)
import logging
logging.basicConfig(level=logging.INFO)

import rank_cmd , info_cmd , fee_cmd
info_cmd.register(client)
rank_cmd.register(client)
fee_cmd.register(client)


# --------- helpers
async def require_reply_to_form(event: events.NewMessage.Event) -> Message:
    if not event.is_reply: return None
    msg = await event.get_reply_message(); return msg

async def in_allowed_group(event: events.NewMessage.Event) -> bool:
    if not ESCROW_GROUP_IDS: return True
    chat_id_str = str(getattr(event.chat, "id", "")) if event.chat else ""
    return chat_id_str in ESCROW_GROUP_IDS

def _display_name_from_entity(e) -> str:
    """Prefer First + Last; fallback to @username; finally numeric id."""
    first = getattr(e, "first_name", "") or ""
    last = getattr(e, "last_name", "") or ""
    name = (f"{first} {last}").strip()
    if name:
        return name
    username = getattr(e, "username", None)
    if username:
        return f"@{username}"
    return str(getattr(e, "id", ""))  # last resort


# --------- basic
@client.on(events.NewMessage(pattern=r"^/start$"))
async def start_cmd(event):
    await ensure_indexes()
    await event.respond("I am a Basic Escrow Tracker and Logger for @Exanic!\n Developed by Owner @Xeborn.")

@client.on(events.NewMessage(pattern=r"^/help$"))
async def help_cmd(event):
    await event.respond(
        "📖 Help Menu\n"
        "/escrowers - To get the list of verified admins.\n"
        "/admin (user_id) (limit) <owner> - Make someone an escrower with a deal limit.\n"
        "/unadmin (user_id) <owner> - Remove someone who is an escrower.\n"
        "/add (amount) <escrowers> - Register a deal amount (reply to the deal form).\n"
        "/cut (amount) <admins/owner> - Deduct partial payment from the main amount.\n"
        "/ext (amount) <admins/owner> - Extend the main amount.\n"
        "/close (amount) <admins/owner> - Close the deal and log the message.\n"
        "/shift (deal_id) <admins/owner> - Shift a deal to a new form.\n"
        "/rank - Top 20 by volume.\n"
        "/info - Your profile card.\n"
        "/stats <owner> - Escrower-wise holdings.\n"
        "/gstats - Global statistics.\n"
        "/fees <owner> - Fees earned per escrower."
    )

# --------- /escrowers, /admin, /unadmin
@client.on(events.NewMessage(pattern=r"^/escrowers$"))
async def escrowers_cmd(event):
    cur = COL_ESCROWERS.find()
    escrowers = [e async for e in cur]
    if not escrowers:
        await event.respond("No verified escrowers yet.")
        return
    lines = ["✅ Verified Escrowers:\n"]
    for e in escrowers:
        disp = e.get("display_name") or str(e.get("user_id"))
        limit = e.get("limit", 0)
        lines.append(f"• {disp} ({e.get('user_id')}) — limit: {int(limit) if float(limit).is_integer() else limit}$")
    await event.respond("\n".join(lines))

@client.on(events.NewMessage(pattern=r"^/admin\s+(\d+)\s+(\d+(?:\.\d+)?)$"))
async def admin_cmd(event):
    if not await is_owner(event.sender_id):
        await event.respond("❌ Only owner can use this command.")
        return

    m = event.pattern_match
    user_id = int(m.group(1))
    limit = float(m.group(2))

    # Fetch the target user to get their proper name
    entity = None
    try:
        entity = await client.get_entity(user_id)
    except Exception:
        # If promoting yourself and get_entity failed, fall back to sender
        if user_id == event.sender_id:
            entity = await event.get_sender()

    display_name = _display_name_from_entity(entity) if entity else str(user_id)

    # Upsert AND refresh display_name every time /admin runs
    await COL_ESCROWERS.update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "limit": limit, "display_name": display_name}},
        upsert=True,
    )

    shown_limit = int(limit) if float(limit).is_integer() else limit
    await event.respond(f"Hence user {user_id} became escrower with a limit of {shown_limit}$.")

@client.on(events.NewMessage(pattern=r"^/unadmin\s+(\d+)$"))
async def unadmin_cmd(event):
    if not await is_owner(event.sender_id):
        await event.respond("❌ Only owner can use this command."); return
    uid = int(event.pattern_match.group(1))
    res = await COL_ESCROWERS.delete_one({"user_id": uid})
    if res.deleted_count:
        await event.respond(f"✅ Removed escrower: {uid}")
    else:
        await event.respond(f"❌ User {uid} is not an escrower.")

# --------- deal form listener (capture seller/buyer only)
@client.on(events.NewMessage())
async def form_listener(event):
    if not await in_allowed_group(event): return
    text = event.raw_text or ""
    if not any(k in text.lower() for k in ["seller -", "buyer -"]): return
    from parsing import parse_deal_form
    parsed = parse_deal_form(text)
    if not parsed: return
    # store minimal stub for traceability (no amount here)
    await COL_DEALS.insert_one({
        "deal_id": None,
        "escrower_id": None,
        "escrower_name": None,
        "buyer_username": parsed["buyer_username"].lower(),
        "seller_username": parsed["seller_username"].lower(),
        "amount": 0.0,
        "main_amount": 0.0,
        "fee": 0.0,
        "remaining": 0.0,
        "status": "pending",
        "form_chat_id": getattr(event.chat,'id',None),
        "form_message_id": event.message.id,
        "created_at": __import__("datetime").datetime.utcnow(),
    })

# --------- /add (escrower only; reply to form)

def _new_deal_id() -> str:
    return "DL-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _display_name_from_entity(e) -> str:
    """Prefer First + Last; fallback to @username; finally numeric id."""
    first = getattr(e, "first_name", "") or ""
    last = getattr(e, "last_name", "") or ""
    name = (f"{first} {last}").strip()
    if name:
        return name
    username = getattr(e, "username", None)
    if username:
        return f"@{username}"
    return str(getattr(e, "id", ""))  # last resort

from telethon.tl.functions.users import GetFullUserRequest

def _normalize_handle(u: str | None) -> str | None:
    if not u:
        return None
    return u.strip().lstrip("@").lower()


# Match @exanic as a standalone token (case-insensitive).
# Allows optional '@' and word boundaries so 'mexanicon' won't match.
_EXANIC_TOKEN = re.compile(r'(?<![A-Za-z0-9_])@?exanic(?![A-Za-z0-9_])', re.IGNORECASE)

# Zero-width + BOM chars that can break matching when users copy/paste
_ZW_CHARS = "".join([
    "\u200B",  # ZERO WIDTH SPACE
    "\u200C",  # ZERO WIDTH NON-JOINER
    "\u200D",  # ZERO WIDTH JOINER
    "\u2060",  # WORD JOINER
    "\uFEFF",  # ZERO WIDTH NO-BREAK SPACE (BOM)
])

def _normalize_handle(u: str | None) -> str | None:
    if not u:
        return None
    return u.strip().lstrip("@").lower()

def _clean_text(s: str | None) -> str:
    if not s:
        return ""
    # Normalize unicode and strip zero-width characters
    s = unicodedata.normalize("NFKC", s)
    return s.translate({ord(c): None for c in _ZW_CHARS})

async def _user_has_exanic_in_bio(client, username: str | None) -> bool:
    """
    True iff the user's BIO contains '@exanic' (case-insensitive).
    Robust to '@' missing, zero-width chars, unicode normalization.
    """
    handle = _normalize_handle(username)
    if not handle:
        return False
    try:
        entity = await client.get_entity(handle)
        full = await client(GetFullUserRequest(entity))
        about = getattr(getattr(full, "full_user", None), "about", "") or ""
        about = _clean_text(about)
        return bool(_EXANIC_TOKEN.search(about))
    except Exception:
        # Username not found / privacy / transient error → treat as no badge
        return False




async def compute_fee(client, buyer_username: str, seller_username: str) -> float:
    b = await _user_has_exanic_in_bio(client, buyer_username)
    s = await _user_has_exanic_in_bio(client, seller_username)
    if b and s:
        return 1
    else:
        return 2    

#------------/add--------------------------
@client.on(events.NewMessage(pattern=r"^/add\s+([0-9]+(\.[0-9]+)?)$"))
async def add_cmd(event: events.NewMessage.Event):
    # 1) Only escrowers can /add
    if not await is_escrower(event.sender_id):
        await event.respond("❌ Only escrowers can use /add.")
        return

    # 2) Must reply to a deal form
    if not event.is_reply:
        await event.respond("❌ You must reply to a deal form with /add.")
        return

    try:
        main_amount = float(event.pattern_match.group(1))
    except Exception:
        await event.respond("❌ Invalid amount.")
        return

    form_msg: Message = await event.get_reply_message()
    text = form_msg.raw_text or ""

    # 3) Extract seller/buyer
    seller_match = re.search(r"(?mi)^\s*Seller\s*-\s*@?([A-Za-z0-9_]{1,32})", text)
    buyer_match = re.search(r"(?mi)^\s*Buyer\s*-\s*@?([A-Za-z0-9_]{1,32})", text)
    seller_username = seller_match.group(1) if seller_match else None
    buyer_username = buyer_match.group(1) if buyer_match else None

    if not (seller_username and buyer_username):
        await event.respond("❌ Could not extract seller/buyer usernames from the form.")
        return

    # 4) Escrower display name
    esc = await COL_ESCROWERS.find_one({"user_id": event.sender_id})
    if esc and esc.get("display_name"):
        escrower_name = esc["display_name"]
    else:
        sender = await event.get_sender()
        escrower_name = sender.first_name or sender.username or str(sender.id)

    # 5) Create deal (deal_logic handles DB + fee)
    deal = await create_deal_from_form(
        client=event.client,
        form_message=form_msg,
        escrower_id=event.sender_id,
        escrower_name=escrower_name,
        buyer_username=buyer_username,
        seller_username=seller_username,
        main_amount=main_amount,
    )

    # 6) Calculate net release amount
    release_amt = deal["main_amount"] - deal["fee"]

    # 7) Escrow card (reply under the form)
    card = (
        f"**Escrow Deal**\n\n"
        f"**ID** - `{deal['deal_id']}`\n"
        f"**Escrower** - {deal['escrower_name']}\n"
        f"**Seller** - @{deal['seller_username']}\n"
        f"**Buyer** - @{deal['buyer_username']}\n"
        f"**Amount** - ${deal['main_amount']:.2f}\n"
        f"**Total Fees** - ${deal['fee']:.2f}\n\n"
        f"**${release_amt:.2f} to be released!**"
    )

    await form_msg.reply(card)

    # 8) Delete the /add command
    try:
        await event.delete()
    except Exception:
        pass

# --------- helpers to get current deal from a card reply
async def deal_from_card_reply(event):
    if not event.is_reply: return None
    reply = await event.get_reply_message()
    # find by deal_id in replied text
    m = __import__('re').search(r"ID\s*-\s*(DL-[A-Z0-9]{6})", reply.raw_text or "")
    if not m: return None
    deal_id = m.group(1)
    deal = await COL_DEALS.find_one({"deal_id": deal_id})
    return deal

# --------- /cut
@client.on(events.NewMessage(pattern=r"^/cut\s+(\d+(?:\.\d+)?)$"))
async def cut_cmd(event):
    # Only admins/owners can cut
    if not await is_admin_or_owner(event.sender_id):
        await event.respond("❌ You are not allowed to use this command.")
        return

    # Must reply to an Escrow Deal card
    deal = await deal_from_card_reply(event)
    if not deal:
        await event.respond("❌ Reply to the Escrow Deal card to use this command.")
        return

    if deal.get("status") == "closed":
        await event.respond("❌ This deal is already closed.")
        return

    # Parse cut amount
    cut_amt = float(event.pattern_match.group(1))

    # Remaining is always tracked against main_amount (the escrow pool)
    remaining = float(deal.get("remaining", 0.0))
    if cut_amt > remaining:
        await event.respond(f"❌ Cut exceeds remaining hold. Remaining: {remaining:.2f}$")
        return

    # Deduct from remaining
    new_remaining = remaining - cut_amt
    await COL_DEALS.update_one(
        {"_id": deal["_id"]},
        {"$set": {"remaining": new_remaining}}
    )

    # Release amount is just the remaining escrow pool (main_amount - cuts)
    fee = float(deal.get("fee", 0.0))
    release_amount = round(new_remaining, 2) - fee

    # Build reply
    await event.respond(
        f"✔ Cut {cut_amt:.2f}$ from Deal {deal['deal_id']}\n"
        f"Remaining Hold: {new_remaining:.2f}$\n\n"
        f"~ {release_amount:.2f}$ to be released"
    )

@client.on(events.NewMessage(pattern=r"^/ext\s+(\d+(?:\.\d+)?)$"))
async def ext_cmd(event):
    # Only admins/owner can extend
    if not await is_admin_or_owner(event.sender_id):
        await event.respond("❌ You are not allowed to use this command.")
        return

    # Must reply to the Escrow Deal card
    deal = await deal_from_card_reply(event)
    if not deal:
        await event.respond("❌ Reply to the Escrow Deal card to use this command.")
        return
    if deal.get("status") == "closed":
        await event.respond("❌ This deal is already closed.")
        return

    # Parse extension amount
    try:
        add_amt = float(event.pattern_match.group(1))
        if add_amt <= 0:
            raise ValueError
    except Exception:
        await event.respond("❌ Invalid amount.")
        return

    # Current values (your /add sets: amount == main_amount, remaining == (main_amount - fee))
    old_main = float(deal.get("main_amount", 0.0))
    old_remaining = float(deal.get("remaining", 0.0))

    # Apply extension: main increases by add_amt; remaining also increases by add_amt (fee unchanged)
    new_main = old_main + add_amt
    new_remaining = old_remaining + add_amt

    # Persist updates
    await COL_DEALS.update_one(
        {"_id": deal["_id"]},
        {"$set": {
            "main_amount": new_main,
            "amount": new_main,          # keep amount in sync with your /add behavior
            "remaining": new_remaining
        }}
    )

    # Acknowledge
    xd = new_remaining-1
    await event.respond(
        f"**Extended {add_amt:.2f}$ to Deal** {deal['deal_id']}\n"
        f"**New Hold:** {new_main:.2f}$\n\n"
        f"~ {xd:.2f}$ to be released."
    )

# --------- /shift (admins/owner), reply to NEW form

@client.on(events.NewMessage(pattern=r"^/shift\s+(DL-[A-Z0-9]{6})$"))
async def shift_cmd(event):
    # only admins/owner can shift
    if not await is_admin_or_owner(event.sender_id):
        await event.respond("❌ Only admins/owner can use /shift.")
        return
    if not event.is_reply:
        await event.respond("❌ Reply to a NEW form with /shift <old_deal_id>.")
        return

    old_deal_id = event.pattern_match.group(1).upper()

    # fetch old deal
    old_deal = await COL_DEALS.find_one({"deal_id": old_deal_id, "status": {"$in": ["pending", "active"]}})
    if not old_deal:
        await event.respond(f"❌ Old deal {old_deal_id} not found or already closed.")
        return

    # reply must be a new deal form (with new buyer)
    form_msg = await event.get_reply_message()
    text = form_msg.raw_text or ""
    import re
    buyer_match = re.search(r"(?mi)^\s*Buyer\s*-\s*@?([A-Za-z0-9_]{1,32})", text)
    new_buyer = buyer_match.group(1) if buyer_match else None
    if not new_buyer:
        await event.respond("❌ Could not parse new buyer username from form.")
        return

    # compute base fee again (from bios), then add +1$ shift fee
    base_fee = await compute_fee(client, new_buyer, old_deal["seller_username"])
    fee = base_fee + 1.0

    new_deal_id = _new_deal_id()
    total = float(old_deal["main_amount"]) - fee

    # insert new deal
    new_deal = {
        "deal_id": new_deal_id,
        "escrower_id": event.sender_id,
        "escrower_name": str(event.sender.first_name or event.sender.id),
        "buyer_username": new_buyer.lower(),
        "seller_username": old_deal["seller_username"].lower(),
        "amount": total,
        "main_amount": float(old_deal["main_amount"]),
        "fee": fee,
        "remaining": float(old_deal["main_amount"]),
        "status": "active",
        "created_at": datetime.now(UTC),
        "form_chat_id": getattr(form_msg.chat, "id", None),
        "form_message_id": form_msg.id,
    }
    await COL_DEALS.insert_one(new_deal)

    # update old deal
    await COL_DEALS.update_one({"_id": old_deal["_id"]}, {"$set": {"status": "shifted", "shifted_to": new_deal_id}})

    # ensure users exist
    await COL_USERS.update_one(
        {"username": new_buyer.lower()},
        {"$setOnInsert": {"created_at": datetime.now(UTC)}},
        upsert=True,
    )

    await event.respond(
        f"🔄 Deal {old_deal_id} has been shifted!\n"
        f"**Escrow Deal**\n\n"
        f"**New ID** - `{new_deal_id}`\n"
        f"**Escrower**- {new_deal['escrower_name']}\n"
        f"**Seller** - @{new_deal['seller_username']}\n"
        f"**Buyer** - @{new_deal['buyer_username']}\n"
        f"**Amount** - {new_deal['main_amount']:.2f}$\n"
        f"**Total Fees** - {new_deal['fee']:.2f}$\n\n"
        f"**{new_deal['amount']:.2f}$ to be released!**"
    )

# --------- /info (global rank by volume)

def _extract_username_from_sender(sender) -> str | None:
    # classic primary username
    if getattr(sender, "username", None):
        return str(sender.username).lstrip("@")
    # collectible usernames list (Fragment)
    user_list = getattr(sender, "usernames", None)
    if user_list:
        for u in user_list:
            if getattr(u, "active", False) and getattr(u, "username", None):
                return str(u.username).lstrip("@")
        for u in user_list:
            if getattr(u, "username", None):
                return str(u.username).lstrip("@")
    return None

async def _upsert_user(db, uid: int | None, uname: str | None) -> None:
    now = datetime.now(UTC)
    doc_set = {"updated_at": now}
    if uid is not None:
        doc_set["user_id"] = uid
    if uname:
        doc_set["username"] = uname.lower()
    if uid:
        await db["users"].update_one(
            {"user_id": uid},
            {"$set": doc_set, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
    elif uname:
        await db["users"].update_one(
            {"username": uname.lower()},
            {"$set": doc_set, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )


# --------- /stats (owner)
@client.on(events.NewMessage(pattern=r"^/stats$"))
async def stats_cmd(event):
    if not await is_escrower(event.sender_id):
        await event.respond("❌ Only escrowers can use this command.")
        return
    holds = await escrower_holdings(db)
    lines = ["✅ Current Escrower-Wise Holdings:\n"]
    for k, v in holds.items():
        lines.append(f"{k} - {v:.3f}$")
    await event.respond("\n".join(lines))

# --------- /gstats (everyone)
@client.on(events.NewMessage(pattern=r"^/gstats$"))
async def gstats_cmd(event):
    total, count, avg = await global_stats(db)
    await event.respond(
        "📊 **Global Statistics:**\n\n"
        f"💸 **Total Amount:** ${total:.2f}\n"
        f"📢 **Total Escrows**: {count}\n"
        f"🔰 **Average Amount**: ${avg:.2f}\n\n"
        f"{FOOTER_INFO_DATE}"
    )

# --------- /fees (owner)
@client.on(events.NewMessage(pattern=r"^/fees$"))
async def fees_cmd(event):
    if not await is_escrower(event.sender_id):
        await event.respond("❌ Only escrowers can use this command.")
        return
    rows = await fees_by_escrower(db)
    if not rows:
        await event.respond("No fees recorded yet.")
        return
    lines = ["💰 Fees Earned (All-time):\n"]
    for r in rows:
        name = r["_id"].get("escrower_name", "") or str(r["_id"].get("escrower_id", ""))
        uid = r["_id"].get("escrower_id", "")
        lines.append(f"{name} ({uid}) — ${float(r.get('total_fees', 0)):.2f} • {int(r.get('deals', 0))} deals")
    await event.respond("\n".join(lines))

# --------- main
async def main():
    try:
        await ensure_indexes()
    except Exception as e:
        print("\n[STARTUP] ensure_indexes() failed:", repr(e))
        traceback.print_exc()
        return

    # Start Telethon client INSIDE the running loop
    try:
        await client.start(bot_token=BOT_TOKEN)
    except Exception as e:
        print("\n[STARTUP] client.start() failed:", repr(e))
        traceback.print_exc()
        return

    print("Escrow bot is running…")
    try:
        await client.run_until_disconnected()
    except Exception as e:
        print("\n[RUNTIME] client.run_until_disconnected() failed:", repr(e))
        traceback.print_exc()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print("\n[TOP-LEVEL] asyncio.run(main()) failed:", repr(e))
        traceback.print_exc()
