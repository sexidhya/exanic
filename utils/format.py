import re
from typing import Optional


USERNAME_RE = re.compile(r"@([A-Za-z0-9_]{1,32})")


def normalize_username(u: Optional[str]) -> Optional[str]:
    if not u:
        return None
    u = u.strip()
    if u.startswith("@"):
        u = u[1:]
    return u.lower() or None


def compact_usd(amount: float) -> str:
    n = float(amount or 0)
    if n >= 1_000_000_000:
        s = f"{n/1_000_000_000:.1f}b$"
    elif n >= 1_000_000:
        s = f"{n/1_000_000:.1f}m$"
    elif n >= 1_000:
        s = f"{n/1_000:.1f}k$"
    else:
        s = f"{int(n)}$" if n.is_integer() else f"{n}$"
    return s.rstrip("0").rstrip(".") if s.endswith(('.0b$', '.0m$', '.0k$')) else s

def mask_name(name: str) -> str:
    clean = (name or "").lstrip("@")
    n = len(clean)
    if n == 0:
        return "(unknown)"
    if n <= 2:
        return clean[0] + "*" * (n - 1)
    if n <= 4:
        return clean[0] + "*" * (n - 2) + clean[-1]
    return clean[0] + "*" * (n - 4) + clean[-3:]
