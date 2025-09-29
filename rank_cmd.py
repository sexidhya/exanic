# rank_cmd.py
from telethon import events
from db import COL_USERS
import logging

# import your rank functions (your file is named rank.py)
from rank import get_top20_by_volume

log = logging.getLogger("rank_cmd")

def register(client):
    # Accept /rank, /rank 10, /rank@Bot, /rank@Bot 10
    pattern = r"^/rank(?:@[\w_]+)?(?:\s+(\d+))?$"

    @client.on(events.NewMessage(pattern=pattern))
    async def rank_handler(event):
        try:
            log.info("[/rank] handler triggered from chat %s by %s", event.chat_id, event.sender_id)
            # N rows (default 20, cap 50)
            m = event.pattern_match
            n = int(m.group(1)) if (m and m.group(1)) else 20
            n = max(1, min(n, 50))

            db = COL_USERS.database  # AsyncIOMotorDatabase
            rows = await get_top20_by_volume(db)  # [{user_id, name, total_volume}, ...]

            if not rows:
                await event.reply("🏆 Top by Escrowed Volume\nNo data yet.")
                return

            lines = ["🏆 Top by Escrowed Volume"]
            for i, r in enumerate(rows[:n], 1):
                name = (r.get("name") or str(r.get("user_id", ""))).strip() or str(r.get("user_id", ""))
                amt = float(r.get("total_volume", 0.0))
                lines.append(f"{i}. {name} - ${amt:,.2f}")

            await event.reply("\n".join(lines))

        except Exception as e:
            # Show the actual error so we know what's wrong
            log.exception("[/rank] error")
            await event.reply(f"❌ /rank error: {e}")
