# info_cmd.py
from telethon import events
from db import COL_USERS
from info import build_info_card  # id-based version

def register(client):
    @client.on(events.NewMessage(pattern=r"^/info(?:@[\w_]+)?(?:\s+(\S+))?$"))
    async def info_handler(event):
        """
        Usage:
          /info                → shows info about yourself
          /info <user_id>      → shows info about that ID
          /info <username>     → shows info about that username
          (reply + /info)      → shows info about the replied user
        """
        arg = event.pattern_match.group(1)
        user_id = None

        if arg:
            if arg.isdigit():  # numeric user_id
                user_id = int(arg)
            else:  # assume username
                uname = arg.lstrip("@").lower()
                doc = await COL_USERS.find_one({"username": uname})
                if doc:
                    user_id = int(doc["user_id"])
        elif event.is_reply:  # replied to someone's msg
            reply_msg = await event.get_reply_message()
            user_id = reply_msg.sender_id
        else:  # fallback: use sender
            user_id = event.sender_id

        if not user_id:
            await event.reply("❌ Could not resolve user.")
            return

        db = COL_USERS.database
        try:
            card = await build_info_card(db, user_id=user_id)
            await event.reply(card)
        except Exception as e:
            print("[/info error]", e)
            await event.reply(f"❌ /info error: {e}")
