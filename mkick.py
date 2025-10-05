from telethon import events
from db import COL_ESCROWERS

async def is_escrower(user_id: int) -> bool:
    doc = await COL_ESCROWERS.find_one({"user_id": user_id})
    return bool(doc)

def register(client):
    @client.on(events.NewMessage(pattern=r"^/mkick\s+(.+)"))
    async def mkick_handler(event):
        # ✅ Restriction check
        if not await is_escrower(event.sender_id):
            await event.reply("⛔You are not authorized to use this command.")
            return

        if not event.is_group:
            await event.reply("⚠️ This command only works in groups.")
            return

        args = event.pattern_match.group(1)
        usernames = [u.strip().lstrip("@") for u in args.split(" ") if u.strip()]

        if not usernames:
            await event.reply("⚠️ No valid usernames provided.")
            return

        results = []
        for uname in usernames:
            try:
                user = await event.client.get_entity(uname)
                await event.client.kick_participant(event.chat_id, user.id)
                results.append(f"✅ Kicked {uname}")
            except Exception as e:
                results.append(f"❌ Failed to kick {uname} ({str(e)})")

        await event.reply("\n".join(results))
