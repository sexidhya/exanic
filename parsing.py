import re
from typing import Optional, Dict

from utils.format import USERNAME_RE


def parse_deal_form(text: str) -> Optional[Dict[str, str]]:
    """Extract ONLY seller/buyer usernames from strict lines 'Seller -' and 'Buyer -'."""
    if not text:
        return None

    seller_match = re.search(r"(?mi)^\s*Seller\s*-\s*(?:@?([A-Za-z0-9_]{1,32}))", text)
    buyer_match = re.search(r"(?mi)^\s*Buyer\s*-\s*(?:@?([A-Za-z0-9_]{1,32}))", text)

    seller = seller_match.group(1) if seller_match else None
    buyer = buyer_match.group(1) if buyer_match else None

    if not (seller and buyer):
        # fallback: first two @handles
        handles = USERNAME_RE.findall(text)
        if len(handles) >= 2:
            seller, buyer = handles[0], handles[1]

    if not (seller and buyer):
        return None

    return {"seller_username": seller, "buyer_username": buyer}
