import random, string
from datetime import datetime
from typing import Dict, Tuple
import re
import unicodedata

# Telethon imports
from telethon.tl.functions.users import GetFullUserRequest

# Database collections
from db import COL_DEALS, COL_USERS

# Import the backend fee helper
from fees import record_fee_from_deal


def _new_deal_id() -> str:
    return "DL-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))


def _normalize_handle(u: str | None) -> str | None:
    if not u:
        return None
    return u.strip().lstrip("@").lower()


# Regex to detect '@exanic' in BIO (case-insensitive, not partial)
_EXANIC_TOKEN = re.compile(r'(?<![A-Za-z0-9_])@?exanic(?![A-Za-z0-9_])', re.IGNORECASE)

# Zero-width characters that can break pattern matching
_ZW_CHARS = "".join([
    "\u200B", "\u200C", "\u200D", "\u2060", "\uFEFF"
])


def _clean_text(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    return s.translate({ord(c): None for c in _ZW_CHARS})


async def _user_has_exanic_in_bio(client, username: str | None) -> bool:
    """
    Returns True if the user's BIO contains '@exanic' (case-insensitive).
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
        # Privacy errors, username not found, etc.
        return False


async def compute_fee(client, buyer_username: str, seller_username: str) -> float:
    """
    Fee logic:
      - If both buyer and seller have '@exanic' in BIO → fee = $1
      - Otherwise → fee = $2
    """
    b = await _user_has_exanic_in_bio(client, buyer_username)
    s = await _user_has_exanic_in_bio(client, seller_username)
    if b and s:
        return 1.0
    else:
        return 2.0


async def create_deal_from_form(
    client,
    form_message,
    escrower_id: int,
    escrower_name: str,
    buyer_username: str,
    seller_username: str,
    main_amount: float
) -> Dict:
    """
    Creates a new deal record and automatically records the fee in the fees collection.
    """
    # Compute dynamic fee
    fee = await compute_fee(client, buyer_username, seller_username)
    total = float(main_amount) + fee

    # Create the deal document
    deal = {
        "deal_id": _new_deal_id(),
        "escrower_id": escrower_id,
        "escrower_name": escrower_name,
        "buyer_username": buyer_username.lower(),
        "seller_username": seller_username.lower(),
        "amount": float(total),          # displayed total (main + fee)
        "main_amount": float(main_amount),
        "fee": float(fee),
        "remaining": float(main_amount), # remaining hold
        "status": "active",
        "created_at": datetime.utcnow(),
        "form_chat_id": getattr(form_message.chat, "id", None),
        "form_message_id": form_message.id,
    }

    # Insert into deals collection
    await COL_DEALS.insert_one(deal)

    # ✅ Automatically record the fee in the fees collection
    try:
        await record_fee_from_deal(deal)
    except Exception as e:
        # Log or print the error but don't block deal creation
        print(f"[WARN] Failed to record fee for deal {deal.get('deal_id')}: {e}")

    # Ensure buyer/seller users exist
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
    """
    Return (amount_display, remaining, release) based on main, fee, cuts.
    """
    main = float(deal.get("main_amount", 0.0))
    fee = float(deal.get("fee", 0.0))
    remaining = float(deal.get("remaining", 0.0))
    amount_display = main + fee
    release = max(0.0, remaining - fee)
    return round(amount_display, 2), round(remaining, 2), round(release, 2)
