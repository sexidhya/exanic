from telethon import events
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import User
from db import COL_DEALS

async def _resolve_user(client, arg: str, fallback_sender):
    """
    Resolve a user from @username, numeric ID, or fallback to sender.
    Returns (user_id, display_name).
    """
    if not arg:
        me = await client(GetFullUserRequest(fallback_sender))
        u: User = me.users[0] if me.users else fallback_sender
        uid = u.id
        name = f"{u.first_name or ''} {u.last_name or ''}".strip() or (u.username or str(uid))
        return uid, name

    handle = arg.strip().lstrip("@").replace("https://t.me/", "").replace("t.me/", "").split("?")[0]

    try:
        entity = await client.get_entity(handle if not handle.isdigit() else int(handle))
        uid = entity.id
        name = f"{getattr(entity,'first_name','') or ''} {getattr(entity,'last_name','') or ''}".strip() \
               or (getattr(entity,'username',None) or str(uid))
        return uid, name
    except Exception:
        me = await client(GetFullUserRequest(fallback_sender))
        u: User = me.users[0] if me.users else fallback_sender
        uid = u.id
        name = f"{u.first_name or ''} {u.last_name or ''}".strip() or (u.username or str(uid))
        return uid, name

def register(client):
    @client.on(events.NewMessage(pattern=r'^/dinfo(?:\s+(\S+))?'))
    async def dinfo_handler(event):
        arg = event.pattern_match.group(1)
        uid, esc_name = await _resolve_user(event.client, arg, event.sender_id)

        # Fetch active deals by escrower
        cur = COL_DEALS.find({"escrower_id": uid, "status": "active"}, {"deal_id": 1, "remaining": 1})
        deals, total_hold = [], 0.0

        async for d in cur:
            deals.append((d.get("deal_id", "N/A"), float(d.get("remaining", 0.0))))
            total_hold += float(d.get("remaining", 0.0))

        deals.sort(key=lambda x: x[1], reverse=True)

        # Build response
        lines = []
        lines.append(f"📊 Deals Info for {esc_name}")
        lines.append("")
        lines.append(f"➥ User ID: {uid}")
        lines.append(f"➥ Total Hold: {total_hold:.1f}$")
        lines.append("")
        lines.append("➥ Deals:" if deals else "➥ Deals: None")

        for deal_id, amt in deals:
            lines.append(f"~ Deal ID: `{deal_id}` → {amt:.1f}$")

        await event.reply("\n".join(lines))
