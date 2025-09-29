import random, string
from datetime import datetime
from typing import Dict, Tuple

# Telethon imports
from telethon.tl.functions.users import GetFullUserRequest

import re
import unicodedata

# Assume these come from your db module
from db import COL_DEALS, COL_USERS

def _new_deal_id() -> str:
    return "DL-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))

# Assume you defined these elsewhere
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


async def create_deal_from_form(
    client,
    form_message,
    escrower_id: int,
    escrower_name: str,
    buyer_username: str,
    seller_username: str,
    main_amount: float
) -> Dict:
    fee = await compute_fee(client, buyer_username, seller_username)
    total = float(main_amount) + fee
    deal = {
        "deal_id": _new_deal_id(),
        "escrower_id": escrower_id,
        "escrower_name": escrower_name,
        "buyer_username": buyer_username.lower(),
        "seller_username": seller_username.lower(),
        "amount": float(total),  # amount displayed in card (main + fee)
        "main_amount": float(main_amount),
        "fee": float(fee),
        "remaining": float(main_amount),  # remaining hold (cuts reduce this)
        "status": "active",
        "created_at": datetime.utcnow(),
        "form_chat_id": getattr(form_message.chat, "id", None),
        "form_message_id": form_message.id,
    }
    await COL_DEALS.insert_one(deal)
    # ensure users exist
    if buyer_username:
        await COL_USERS.update_one(
            {"username": buyer_username.lower()},
            {"$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
        )
    if seller_username:
        await COL_USERS.update_one(
            {"username": seller_username.lower()},
            {"$setOnInsert": {"created_at": datetime.utcnow()}},
            upsert=True,
        )
    return deal


async def recalc_amount_fields(deal: Dict) -> Tuple[float, float, float]:
    """Return (amount_display, remaining, release) based on main, fee, cuts."""
    main = float(deal.get("main_amount", 0.0))
    fee = float(deal.get("fee", 0.0))
    remaining = float(deal.get("remaining", 0.0))
    amount_display = main + fee
    release = max(0.0, remaining - fee)
    return round(amount_display, 2), round(remaining, 2), round(release, 2)
